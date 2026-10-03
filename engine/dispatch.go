package engine

import (
	"context"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"sort"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// runDone guards the lifecycle of the background loop.
//
// Run starts the sweep loop in a background goroutine and returns
// immediately; Stop cancels it and waits until it exits. The loop may
// only be started once per Engine.
func (e *Engine) Run(ctx context.Context) error {
	e.runOnce.Do(func() {
		loopCtx, cancel := context.WithCancel(ctx)
		e.cancelLoop = cancel
		go func() {
			defer close(e.doneCh)
			e.loop(loopCtx)
		}()
	})
	return nil
}

// loop is the blocking sweep loop (queue timeouts, delivery retries).
func (e *Engine) loop(ctx context.Context) {
	t := time.NewTicker(e.opts.SweepInterval)
	defer t.Stop()
	// Run one pass shortly after startup so recovered dues flow quickly.
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case now := <-t.C:
			e.sweep(ctx, now.UTC())
		}
	}
}

// sweep performs one housekeeping pass: time out over-waiting sessions,
// retry queued sessions when capacity freed up, and re-push deliveries
// that saturated inboxes rejected.
func (e *Engine) sweep(ctx context.Context, now time.Time) {
	// 1. Queue-deadline expirations -> fallback with distinguishable
	// reason. The queue entry is removed inside the serialized
	// transition (with rollback), so a lost race can restore it; here
	// we only collect candidates.
	for _, en := range e.q.All() {
		if en.TimedOut(now) {
			_ = e.ReturnToBot(ctx, en.SessionID, domain.ReasonQueueTimeout, now)
		}
	}

	// 2. Opportunistic dispatch of still-queued sessions, oldest first.
	e.dispatchQueued(ctx, now)

	// 3. Retry unacked deliveries that never made it into an inbox.
	e.retryDeliveries(ctx)
}

// dispatchQueued assigns waiting sessions to currently free, eligible
// agents in deterministic global-FIFO order.
func (e *Engine) dispatchQueued(ctx context.Context, now time.Time) {
	for _, en := range e.q.All() {
		if en.TimedOut(now) {
			continue // handled by DueTimeouts
		}
		ag, ok := e.pickAgent(en.Queue)
		if !ok {
			continue
		}
		sys := auth.Principal{ID: "scheduler", Role: auth.RoleSystem}
		if err := e.assignQueued(ctx, sys, en.SessionID, ag, now); err != nil {
			// Reservation lost to a concurrent take or the session moved
			// on: deterministic single-winner semantics mean we just skip.
			continue
		}
	}
}

// assignQueued is the scheduler-side pickup, mirroring Take but driven
// by agent selection rather than the agent itself.
func (e *Engine) assignQueued(ctx context.Context, p auth.Principal, id domain.SessionID, ag domain.AgentID, now time.Time) error {
	return e.mutate(ctx, id, p.ID, "scheduler.assign", func(st *sessionState, now2 time.Time) (mutateOutput, error) {
		if !st.exists || st.sess.Status != domain.StatusQueued {
			return mutateOutput{}, domain.ErrNotQueued
		}
		ok, err := e.reg.TryReserve(ag, id)
		if err != nil || !ok {
			return mutateOutput{}, domain.ErrAgentAtCapacity
		}
		entry, inQueue := e.q.Remove(id)
		return mutateOutput{
			events: []domain.Event{{
				Type:    domain.EvAgentAssigned,
				Actor:   p.ID,
				Reason:  domain.ReasonAgentAccepted,
				AgentID: ag,
				Queue:   entry.Queue,
			}},
			rollback: func() {
				e.reg.Release(ag, id)
				if inQueue {
					_ = e.q.Enqueue(entry)
				}
			},
			rebind: true,
			records: []audit.Record{allowedRecord(p.ID, "scheduler.assign", id, ag,
				domain.ReasonAgentAccepted)},
		}, nil
	})
}

// retryDeliveries re-pushes unacked messages that were not accepted by
// a (possibly saturated) inbox, per session in arrival order.
func (e *Engine) retryDeliveries(ctx context.Context) {
	e.mu.Lock()
	ids := make([]domain.SessionID, 0, len(e.sessions))
	for id := range e.sessions {
		ids = append(ids, id)
	}
	e.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		st, ok := e.getState(id)
		if !ok {
			continue
		}
		st.mu.Lock()
		if st.sess.Status == domain.StatusClosed {
			st.mu.Unlock()
			continue
		}
		ds := e.collectDueLocked(st)
		st.mu.Unlock()
		if len(ds) == 0 {
			continue
		}
		failed := e.dispatchAll(st, ds)
		st.mu.Lock()
		for mid := range failed {
			st.inflight[mid] = false
		}
		st.mu.Unlock()
	}
}

// SetAgentOnline flips agent availability. When an agent comes online,
// waiting sessions are dispatched immediately; when one goes offline,
// its active sessions are returned to the bot with the
// ReasonAgentOffline cause (fail-safe: nobody is left unwatched).
func (e *Engine) SetAgentOnline(ctx context.Context, id domain.AgentID, online bool, at time.Time) error {
	now := at
	if now.IsZero() {
		now = e.now()
	}
	e.reg.SetOnline(id, online)
	if online {
		e.dispatchQueued(ctx, now)
		return nil
	}

	// Offline: find assigned/transferring sessions bound to this agent.
	e.mu.Lock()
	type touch struct {
		id     domain.SessionID
		active bool
	}
	found := make([]touch, 0)
	for sid, st := range e.sessions {
		st.mu.Lock()
		active := st.sess.Status != domain.StatusClosed &&
			(st.sess.Handler.AgentID == id || st.sess.TransferTo == id)
		st.mu.Unlock()
		found = append(found, touch{sid, active})
	}
	e.mu.Unlock()
	ids := make([]domain.SessionID, 0, len(found))
	for _, f := range found {
		if f.active {
			ids = append(ids, f.id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, sid := range ids {
		// ReturnToBot releases every reservation tied to the session and
		// gives the explicit offline reason. If the session moved on
		// concurrently it is a no-op/idempotent result.
		_ = e.ReturnToBot(ctx, sid, domain.ReasonAgentOffline, now)
	}
	return nil
}

// Recover rebuilds all live state from the durable event log and resumes
// unfinished work without repeating completed transitions.
func (e *Engine) Recover(ctx context.Context, at time.Time) error {
	now := at
	if now.IsZero() {
		now = e.now()
	}
	events, err := e.store.ReadAll(ctx)
	if err != nil {
		return err
	}
	// Group by session preserving append order.
	groups := make(map[domain.SessionID][]domain.Event)
	var order []domain.SessionID
	for _, ev := range events {
		if _, ok := groups[ev.SessionID]; !ok {
			order = append(order, ev.SessionID)
		}
		groups[ev.SessionID] = append(groups[ev.SessionID], ev)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	e.mu.Lock()
	e.sessions = make(map[domain.SessionID]*sessionState)
	e.mu.Unlock()

	for _, sid := range order {
		evs := groups[sid]
		st := materialize(sid, evs)
		e.mu.Lock()
		e.sessions[sid] = st
		e.mu.Unlock()

		st.mu.Lock()
		status := st.sess.Status
		ag := st.sess.Handler.AgentID
		transferTarget := st.sess.TransferTo
		qn := st.sess.Queue
		snap := st.snapshotLocked()
		st.mu.Unlock()

		switch status {
		case domain.StatusQueued:
			// Rebuild the waiting entry preserving the original deadline
			// when possible; fall back to a fresh one.
			deadline := time.Time{}
			if e.maxWait > 0 {
				deadline = snap.WaitingSince.Add(e.maxWait)
				if deadline.Before(now) {
					deadline = now.Add(e.maxWait)
				}
			}
			_ = e.q.Enqueue(queueEntry(sid, qn, snap.WaitingSince, deadline))
		case domain.StatusAssigned:
			if !e.reserveRecovered(ag, sid) {
				_ = e.ReturnToBot(ctx, sid, domain.ReasonAgentOffline, now)
				continue
			}
		case domain.StatusTransferring:
			// Target reservation was made before the crash; restore it.
			if !e.reserveRecovered(transferTarget, sid) || !e.reserveRecovered(ag, sid) {
				_ = e.ReturnToBot(ctx, sid, domain.ReasonAgentOffline, now)
				continue
			}
		}
	}
	// Push the redeliveries out and opportunistically dispatch.
	e.retryDeliveries(ctx)
	e.dispatchQueued(ctx, now)
	return nil
}

// reserveRecovered attempts an idempotent capacity re-reservation.
func (e *Engine) reserveRecovered(ag domain.AgentID, sid domain.SessionID) bool {
	if ag == "" {
		return false
	}
	snap, known := e.reg.Get(ag)
	if !known || !snap.Online {
		return false
	}
	ok, err := e.reg.TryReserve(ag, sid)
	return err == nil && ok
}
