package engine

import (
	"context"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"

	"github.com/Diudiuseven/support-agent-handoff-go/queue"
)

// Take is a competitive pickup by a specific agent of a queued session.
//
// Deterministic winner: for the same session every competitor passes
// through the session's single mutex; TryReserve (atomic capacity
// accounting) succeeds for exactly one agent and fails for everyone
// else, so two agents can never both own the session.
func (e *Engine) Take(ctx context.Context, p auth.Principal, id domain.SessionID, at time.Time) error {
	if !e.policy.Can(p, auth.ActionTake, domain.AgentID(p.ID)) {
		return e.deny(ctx, p, auth.ActionTake, id, domain.AgentID(p.ID))
	}
	ag := domain.AgentID(p.ID)

	return e.mutate(ctx, id, p.ID, string(auth.ActionTake), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		switch st.sess.Status {
		case domain.StatusClosed:
			return mutateOutput{}, domain.ErrSessionClosed
		case domain.StatusQueued:
			// expected path: compete for it
		case domain.StatusAssigned:
			if st.sess.Handler.AgentID == ag {
				return mutateOutput{records: []audit.Record{ignoredRecord(p.ID, string(auth.ActionTake), id, "already_owner")}}, nil // idempotent self-take
			}
			return mutateOutput{}, domain.ErrAlreadyAssigned
		default:
			return mutateOutput{}, domain.ErrAlreadyAssigned
		}

		ok, err := e.reg.TryReserve(ag, id)
		if err != nil || !ok {
			if err == domain.ErrAgentAtCapacity {
				return mutateOutput{}, domain.ErrAgentAtCapacity
			}
			if err != nil {
				return mutateOutput{}, err
			}
			return mutateOutput{}, domain.ErrAgentAtCapacity
		}
		// Reserved first; on append failure both the reservation and the
		// queue position are restored.
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
			records: []audit.Record{allowedRecord(p.ID, string(auth.ActionTake), id, ag,
				domain.ReasonAgentAccepted)},
		}, nil
	})
}

// Accept completes a pending agent-to-agent transfer: the target agent
// becomes the unique owner. Only the transfer target may accept.
func (e *Engine) Accept(ctx context.Context, p auth.Principal, id domain.SessionID, at time.Time) error {
	if !e.policy.Can(p, auth.ActionAccept, domain.AgentID(p.ID)) {
		return e.deny(ctx, p, auth.ActionAccept, id, domain.AgentID(p.ID))
	}
	target := domain.AgentID(p.ID)

	return e.mutate(ctx, id, p.ID, string(auth.ActionAccept), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{}, domain.ErrSessionClosed
		}
		if st.sess.Status != domain.StatusTransferring {
			return mutateOutput{}, domain.ErrNotTransferring
		}
		if st.sess.TransferTo != target {
			return mutateOutput{}, domain.ErrUnauthorized
		}
		from := st.sess.Handler.AgentID
		// Capacity for target was reserved when the transfer started;
		// release the previous owner's slot at commit time.
		return mutateOutput{
			events: []domain.Event{{
				Type:    domain.EvTransferCompleted,
				Actor:   p.ID,
				Reason:  domain.ReasonAgentTransferred,
				AgentID: target,
			}},
			after:  func() { e.reg.Release(from, id) },
			rebind: true,
			records: []audit.Record{allowedRecord(p.ID, string(auth.ActionAccept), id, target,
				domain.ReasonAgentTransferred)},
		}, nil
	})
}

// ReturnToBot sends the session back to the bot with a distinguishable
// reason (queue timeout, no agent available, agent offline ...).
func (e *Engine) ReturnToBot(ctx context.Context, id domain.SessionID, reason domain.ReasonCode, at time.Time) error {
	actor := "system"
	return e.mutate(ctx, id, actor, "session.return_to_bot", func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{}, domain.ErrSessionClosed
		}
		if st.sess.Status == domain.StatusBot {
			return mutateOutput{}, nil // idempotent
		}
		var owner domain.AgentID
		var qEntry queue.Entry
		var inQueue bool
		if st.sess.Status == domain.StatusQueued {
			qEntry, inQueue = e.q.Remove(id)
		} else {
			owner = st.sess.Handler.AgentID
		}
		transferTarget := st.sess.TransferTo
		out := mutateOutput{
			events: []domain.Event{{
				Type:   domain.EvReturnedToBot,
				Actor:  actor,
				Reason: reason,
			}},
			rebind: true,
			records: []audit.Record{allowedRecord(actor, "session.return_to_bot", id, "",
				reason)},
			// If the append loses a race the session must reappear in the
			// queue exactly where it was (deadline preserved).
			rollback: func() {
				if inQueue {
					_ = e.q.Enqueue(qEntry)
				}
			},
		}
		out.after = func() {
			if owner != "" {
				e.reg.Release(owner, id)
			}
			if transferTarget != "" {
				// Reserve-before-start side effect is undone too.
				e.reg.Release(transferTarget, id)
			}
		}
		return out, nil
	})
}

// Close terminates the session and releases all held capacity.
func (e *Engine) Close(ctx context.Context, p auth.Principal, id domain.SessionID, reason domain.ReasonCode, at time.Time) error {
	if !e.policy.Can(p, auth.ActionClose, "") {
		return e.deny(ctx, p, auth.ActionClose, id, "")
	}
	if reason == domain.ReasonNone {
		reason = domain.ReasonCustomerEnded
	}
	return e.mutate(ctx, id, p.ID, string(auth.ActionClose), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{records: []audit.Record{ignoredRecord(p.ID, string(auth.ActionClose), id, "already_closed")}}, nil
		}
		// Agent role may only close sessions it owns.
		if p.Role == auth.RoleAgent {
			if st.sess.Status == domain.StatusAssigned && st.sess.Handler.AgentID != domain.AgentID(p.ID) {
				return mutateOutput{}, domain.ErrUnauthorized
			}
		}
		var owner, transferTarget domain.AgentID
		var qEntry queue.Entry
		var inQueue bool
		switch st.sess.Status {
		case domain.StatusQueued:
			qEntry, inQueue = e.q.Remove(id)
		case domain.StatusTransferring:
			owner = st.sess.Handler.AgentID
			transferTarget = st.sess.TransferTo
		case domain.StatusAssigned:
			owner = st.sess.Handler.AgentID
		}
		out := mutateOutput{
			events: []domain.Event{{
				Type:   domain.EvSessionClosed,
				Actor:  p.ID,
				Reason: reason,
			}},
			records: []audit.Record{allowedRecord(p.ID, string(auth.ActionClose), id, "", reason)},
			rollback: func() {
				if inQueue {
					_ = e.q.Enqueue(qEntry)
				}
			},
		}
		out.after = func() {
			if owner != "" {
				e.reg.Release(owner, id)
			}
			if transferTarget != "" {
				e.reg.Release(transferTarget, id)
			}
		}
		return out, nil
	})
}
