package engine

import (
	"context"
	"errors"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// mutateOutput describes one attempted serialized transition.
type mutateOutput struct {
	// events are appended atomically; their Seq is assigned by mutate.
	events []domain.Event
	// rollback reverses side effects (e.g. capacity reservation, queue
	// position) when the transition is rejected or loses a race.
	rollback func()
	// rebind, when true, re-targets all unacked messages to the new
	// owner after the events are applied (ownership-changing paths).
	rebind bool
	// after runs under the session lock right after events are applied
	// (e.g. releasing the previous owner's capacity).
	after func()
	// records are written to the audit trail after successful commit.
	records []audit.Record
}

// undo safely runs the rollback callback.
func (o mutateOutput) undo() {
	if o.rollback != nil {
		o.rollback()
	}
}

// mutate serializes a transition on one session with optimistic
// concurrency. fn is invoked with the live projection locked; it may
// perform external side effects as long as it returns a rollback that
// reverses them. On domain.ErrConflict the projection is rebuilt from
// the store and fn is retried, so concurrent transitions converge to a
// single deterministic outcome. actor/action are used to write a denied
// audit record when fn rejects an operation at the resource layer.
func (e *Engine) mutate(ctx context.Context, id domain.SessionID, actor, action string,
	fn func(st *sessionState, now time.Time) (mutateOutput, error)) error {
	const maxAttempts = 25

	for attempt := 0; attempt < maxAttempts; attempt++ {
		st, err := e.getOrLoad(ctx, id)
		if err != nil {
			return err
		}
		now := e.now()

		st.mu.Lock()
		out, verr := fn(st, now)
		if verr != nil {
			st.mu.Unlock()
			out.undo()
			// Resource-level rejections (wrong owner, wrong state,
			// capacity, ...) are auditable as denials.
			if action != "" {
				_ = e.aud.Log(ctx, audit.Record{
					Actor:   actor,
					Action:  action,
					Session: string(id),
					Outcome: audit.OutcomeDenied,
					Reason:  verr.Error(),
				})
			}
			return verr
		}
		if len(out.events) == 0 {
			// Read-only / idempotent no-op (audit still recorded).
			recs := out.records
			st.mu.Unlock()
			out.undo()
			for _, r := range recs {
				_ = e.aud.Log(ctx, r)
			}
			return nil
		}

		// Assign contiguous seqs and fill timestamps.
		last := st.sess.LastDelivered
		for i := range out.events {
			last++
			out.events[i].Seq = last
			out.events[i].SessionID = id
			if out.events[i].At.IsZero() {
				out.events[i].At = now
			}
		}

		appendErr := e.store.Append(ctx, id, st.sess.LastDelivered, out.events...)
		if errors.Is(appendErr, domain.ErrConflict) {
			st.mu.Unlock()
			out.undo()
			if err := e.reloadSession(ctx, id); err != nil {
				return err
			}
			continue
		}
		if appendErr != nil {
			st.mu.Unlock()
			out.undo()
			return appendErr
		}

		// Commit point: fold events into the projection while ordered.
		for _, ev := range out.events {
			st.applyLocked(ev)
		}
		if out.rebind {
			// New owner is now in st.sess.Handler: re-target unacked
			// messages so they are redelivered in order, never lost.
			st.rebindPendingLocked()
		}
		if out.after != nil {
			out.after()
		}
		// Collect due deliveries while ordered, then release the lock
		// before calling external inboxes.
		deliveries := e.collectDueLocked(st)
		st.mu.Unlock()

		failed := e.dispatchAll(st, deliveries)
		if len(failed) > 0 {
			st.mu.Lock()
			for mid := range failed {
				// Push rejected (saturated/missing inbox): leave due for
				// the sweep; other sessions were never blocked.
				st.inflight[mid] = false
			}
			st.mu.Unlock()
		}
		for _, r := range out.records {
			_ = e.aud.Log(ctx, r)
		}
		return nil
	}
	return domain.ErrConflict
}

// reloadSession rebuilds one session's projection from the durable log
// and swaps it into the live map.
func (e *Engine) reloadSession(ctx context.Context, id domain.SessionID) error {
	events, err := e.store.ReadSession(ctx, id)
	if err != nil {
		return err
	}
	st := materialize(id, events)
	e.mu.Lock()
	e.sessions[id] = st
	e.mu.Unlock()
	return nil
}

// collectDueLocked gathers messages that must be delivered right now:
// persisted + unacked + not already in flight, in arrival order, and
// marks them in flight. Caller holds st.mu.
func (e *Engine) collectDueLocked(st *sessionState) []Delivery {
	out := make([]Delivery, 0)
	h := st.sess.Handler
	for _, id := range st.pendingOrder {
		p, ok := st.pending[id]
		if !ok || st.inflight[id] {
			continue
		}
		if !sameHandler(p.Handler, h) {
			// Projection/rebind bookkeeping catch-up.
			p.Handler = h
			st.pending[id] = p
			st.redelivered[id] = true
		}
		out = append(out, Delivery{
			SessionID:   st.sess.ID,
			Msg:         p.Msg,
			Handler:     h,
			Redelivered: st.redelivered[id],
		})
		st.inflight[id] = true
	}
	return out
}

func sameHandler(a, b domain.Handler) bool {
	return a.Kind == b.Kind && (a.Kind != domain.HandlerAgent || a.AgentID == b.AgentID)
}

// dispatchAll attempts non-blocking delivery of every due message in
// order. It returns the set of message ids rejected by their inbox
// (saturated / missing); those remain due for sweep retries.
func (e *Engine) dispatchAll(st *sessionState, ds []Delivery) map[string]bool {
	failed := make(map[string]bool)
	for _, d := range ds {
		ib := e.inboxFor(d.Handler)
		if ib == nil || !ib.TryDeliver(d) {
			failed[d.Msg.ID] = true
		}
	}
	return failed
}
