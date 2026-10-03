package engine

import (
	"context"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TransferTo starts an atomic agent-to-agent transfer.
//
// Ownership/ordering rules:
//
//   - While the status is StatusTransferring the current owner does not
//     change, so messages arriving mid-transfer keep going to the owner
//     in order — nothing is misrouted.
//   - The target's capacity is reserved up front, atomically, so two
//     agents can never both end up holding the session.
//   - Accept flips ownership in one serialized transition; unacked
//     messages are then rebound to the new owner and redelivered in
//     their original order.
//   - Cancel (or ReturnToBot/Close) releases the target reservation.
func (e *Engine) TransferTo(ctx context.Context, p auth.Principal, id domain.SessionID, target domain.AgentID, at time.Time) error {
	if target == "" {
		return domain.ErrInvalidArgument
	}
	if !e.policy.Can(p, auth.ActionTransfer, target) {
		return e.deny(ctx, p, auth.ActionTransfer, id, target)
	}
	return e.mutate(ctx, id, p.ID, string(auth.ActionTransfer), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{}, domain.ErrSessionClosed
		}
		if st.sess.Status != domain.StatusAssigned {
			return mutateOutput{}, domain.ErrAlreadyAssigned
		}
		// Only the current owner, admin or system may initiate.
		owner := st.sess.Handler.AgentID
		if p.Role == auth.RoleAgent && owner != domain.AgentID(p.ID) {
			return mutateOutput{}, domain.ErrUnauthorized
		}
		if target == owner {
			return mutateOutput{}, domain.ErrInvalidArgument
		}
		if _, known := e.reg.Get(target); !known {
			return mutateOutput{}, domain.ErrAgentOffline
		}
		ok, err := e.reg.TryReserve(target, id)
		if err != nil {
			return mutateOutput{}, err
		}
		if !ok {
			return mutateOutput{}, domain.ErrAgentAtCapacity
		}
		return mutateOutput{
			events: []domain.Event{{
				Type:    domain.EvTransferStarted,
				Actor:   p.ID,
				Reason:  domain.ReasonAgentTransferred,
				AgentID: target,
			}},
			rollback: func() { e.reg.Release(target, id) },
			records:  []audit.Record{allowedRecord(p.ID, string(auth.ActionTransfer), id, target, domain.ReasonAgentTransferred)},
		}, nil
	})
}

// CancelTransfer aborts an in-flight transfer; the current owner keeps
// the session. The initiator, the target or an admin may cancel.
func (e *Engine) CancelTransfer(ctx context.Context, p auth.Principal, id domain.SessionID, at time.Time) error {
	if !e.policy.Can(p, auth.ActionTransfer, "") {
		return e.deny(ctx, p, auth.ActionTransfer, id, "")
	}
	return e.mutate(ctx, id, p.ID, string(auth.ActionTransfer)+".cancel", func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status != domain.StatusTransferring {
			return mutateOutput{}, domain.ErrNotTransferring
		}
		target := st.sess.TransferTo
		if p.Role == auth.RoleAgent {
			owner := st.sess.Handler.AgentID
			if domain.AgentID(p.ID) != owner && domain.AgentID(p.ID) != target {
				return mutateOutput{}, domain.ErrUnauthorized
			}
		}
		return mutateOutput{
			events: []domain.Event{{
				Type:    domain.EvTransferCanceled,
				Actor:   p.ID,
				Reason:  domain.ReasonAgentRejected,
				AgentID: target,
			}},
			after:   func() { e.reg.Release(target, id) },
			records: []audit.Record{allowedRecord(p.ID, string(auth.ActionTransfer)+".cancel", id, target, domain.ReasonAgentRejected)},
		}, nil
	})
}
