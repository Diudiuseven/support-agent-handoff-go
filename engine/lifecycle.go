package engine

import (
	"context"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"

	"github.com/Diudiuseven/support-agent-handoff-go/auth"
)

// deny audits a permission failure and returns ErrUnauthorized.
func (e *Engine) deny(ctx context.Context, p auth.Principal, act auth.Action, sid domain.SessionID, target domain.AgentID) error {
	_ = e.aud.Log(ctx, audit.Record{
		Actor:   p.ID,
		Action:  string(act),
		Session: string(sid),
		Target:  string(target),
		Outcome: audit.OutcomeDenied,
		Reason:  string(domain.ReasonUnauthorized),
	})
	return domain.ErrUnauthorized
}

// denyResource audits an engine-level resource denial (wrong owner,
// wrong state, ...) and returns the original error.
func (e *Engine) denyResource(ctx context.Context, actor, action string, sid domain.SessionID, target domain.AgentID, err error) error {
	_ = e.aud.Log(ctx, audit.Record{
		Actor:   actor,
		Action:  action,
		Session: string(sid),
		Target:  string(target),
		Outcome: audit.OutcomeDenied,
		Reason:  err.Error(),
	})
	return err
}

// allowedRecord builds an audit record for a permitted transition.
func allowedRecord(actor, action string, sid domain.SessionID, target domain.AgentID, reason domain.ReasonCode) audit.Record {
	return audit.Record{
		Actor:   actor,
		Action:  action,
		Session: string(sid),
		Target:  string(target),
		Outcome: audit.OutcomeAllowed,
		Reason:  string(reason),
	}
}

// ignoredRecord marks a benign duplicate/idempotent no-op.
func ignoredRecord(actor, action string, sid domain.SessionID, reason string) audit.Record {
	return audit.Record{
		Actor:   actor,
		Action:  action,
		Session: string(sid),
		Outcome: audit.OutcomeIgnored,
		Detail:  reason,
	}
}

// Open starts a new session owned by the bot.
func (e *Engine) Open(ctx context.Context, id domain.SessionID, queueName domain.QueueName, at time.Time) error {
	if id == "" {
		return domain.ErrInvalidArgument
	}
	now := at
	if now.IsZero() {
		now = e.now()
	}
	ev := domain.Event{
		SessionID: id,
		Type:      domain.EvSessionOpened,
		At:        now,
		Actor:     "system",
		Queue:     queueName,
	}
	// expected seq -1 means "must not exist yet", so duplicate Opens
	// fail with domain.ErrConflict.
	if err := e.store.Append(ctx, id, -1, ev); err != nil {
		return err
	}
	st := materialize(id, []domain.Event{{
		Seq: 0, SessionID: id, Type: domain.EvSessionOpened,
		At: now, Actor: "system", Queue: queueName,
	}})
	e.mu.Lock()
	if _, exists := e.sessions[id]; exists {
		// Recovered/loaded concurrently with the append; keep the
		// canonical projection.
		e.mu.Unlock()
		return nil
	}
	e.sessions[id] = st
	e.mu.Unlock()
	return nil
}

// Escalate moves a bot-owned (or fallback) session toward a human.
//
// Determinism: agents are chosen by a total order (id ascending) among
// the online, queue-serving, under-capacity agents with the lowest
// current load. If one can be reserved immediately the session is
// assigned; otherwise it is enqueued with a deadline. The capacity
// reservation and event append happen inside one serialized section.
func (e *Engine) Escalate(ctx context.Context, p auth.Principal, id domain.SessionID, reason domain.ReasonCode, at time.Time) (HandoffResult, error) {
	if !e.policy.Can(p, auth.ActionEscalate, "") {
		return HandoffResult{}, e.deny(ctx, p, auth.ActionEscalate, id, "")
	}
	if reason == domain.ReasonNone {
		reason = domain.ReasonCustomerRequested
	}
	res := HandoffResult{SessionID: id}

	err := e.mutate(ctx, id, p.ID, string(auth.ActionEscalate), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{}, domain.ErrSessionClosed
		}
		if st.sess.Status == domain.StatusQueued {
			// Already waiting: idempotent report, no new events.
			res.Status = domain.StatusQueued
			res.Queue = st.sess.Queue
			res.Reason = domain.ReasonNone
			return mutateOutput{records: []audit.Record{ignoredRecord(p.ID, string(auth.ActionEscalate), id, "already_queued")}}, nil
		}
		if st.sess.Status == domain.StatusAssigned || st.sess.Status == domain.StatusTransferring {
			return mutateOutput{}, domain.ErrAlreadyAssigned
		}

		qn := st.sess.Queue
		if a, ok := e.pickAgent(qn); ok {
			ok2, rerr := e.reg.TryReserve(a, id)
			if rerr != nil || !ok2 {
				// Extremely rare (state changed under us); fall through
				// to queueing rather than failing the escalation.
				a = ""
			}
			if a != "" {
				res.Status = domain.StatusAssigned
				res.AssignedAgent = a
				res.Queue = qn
				res.Reason = domain.ReasonAgentAccepted
				return mutateOutput{
					events: []domain.Event{{
						Type:    domain.EvAgentAssigned,
						Actor:   p.ID,
						Reason:  reason,
						AgentID: a,
						Queue:   qn,
					}},
					rollback: func() { e.reg.Release(a, id) },
					rebind:   true,
					records: []audit.Record{allowedRecord(p.ID, string(auth.ActionEscalate), id, a,
						domain.ReasonAgentAccepted)},
				}, nil
			}
		}

		// Queue the session.
		deadline := time.Time{}
		if e.maxWait > 0 {
			deadline = now.Add(e.maxWait)
		}
		entry := queue.Entry{SessionID: id, Queue: qn, EnqueuedAt: now, Deadline: deadline}
		if err := e.q.Enqueue(entry); err != nil {
			return mutateOutput{}, err
		}
		res.Status = domain.StatusQueued
		res.Queue = qn
		res.Reason = reason
		return mutateOutput{
			events: []domain.Event{{
				Type:   domain.EvEscalatedToQueue,
				Actor:  p.ID,
				Reason: reason,
				Queue:  qn,
			}},
			rollback: func() { _, _ = e.q.Remove(id) },
			records:  []audit.Record{allowedRecord(p.ID, string(auth.ActionEscalate), id, "", reason)},
		}, nil
	})
	if err != nil {
		return HandoffResult{}, err
	}
	return res, nil
}

// pickAgent deterministically selects the best candidate for a queue:
// minimum (load, id) among online agents serving the queue with spare
// capacity.
func (e *Engine) pickAgent(qn domain.QueueName) (domain.AgentID, bool) {
	snaps := e.reg.List()
	best := -1
	for i, s := range snaps {
		if !s.Online || !s.Available() {
			continue
		}
		if !serves(s.Profile.Queues, qn) {
			continue
		}
		if best == -1 || lessCandidate(s, snaps[best]) {
			best = i
		}
	}
	if best < 0 {
		return "", false
	}
	return snaps[best].Profile.ID, true
}

func lessCandidate(a, b agent.Snapshot) bool {
	if a.Load != b.Load {
		return a.Load < b.Load
	}
	return a.Profile.ID < b.Profile.ID
}

func serves(queues []domain.QueueName, want domain.QueueName) bool {
	for _, q := range queues {
		if q == want {
			return true
		}
	}
	return false
}
