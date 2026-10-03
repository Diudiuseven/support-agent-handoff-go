package engine

import (
	"context"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// Send persists a message and routes it.
//
// Durability/ordering: the EvDelivered event is appended before any
// external delivery, so messages arriving right before or during a
// handoff can neither be lost nor re-ordered: they are read back from
// the log and rebound to the new owner. Handlers must use the message
// ID as an idempotency key (at-least-once with per-session order).
//
// Routing:
//
//   - customer/system messages are inbound and pushed to whoever
//     currently owns the session (bot or agent);
//   - bot/agent messages are outbound replies: they are recorded but
//     not pushed back to the bot, and auto-acknowledged.
func (e *Engine) Send(ctx context.Context, p auth.Principal, msg domain.Message) error {
	if msg.SessionID == "" || msg.ID == "" {
		return domain.ErrInvalidArgument
	}
	if !e.policy.Can(p, auth.ActionSend, "") {
		return e.deny(ctx, p, auth.ActionSend, msg.SessionID, "")
	}
	if msg.Origin == domain.OriginAgent {
		msg.Sender = domain.AgentID(p.ID)
	}
	return e.mutate(ctx, msg.SessionID, p.ID, string(auth.ActionSend), func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if st.sess.Status == domain.StatusClosed {
			return mutateOutput{}, domain.ErrSessionClosed
		}
		if msg.At.IsZero() {
			msg.At = now
		}
		msg.SessionID = st.sess.ID

		switch msg.Origin {
		case domain.OriginCustomer, domain.OriginSystem:
			// Handled by the generic delivery path in mutate.
			return mutateOutput{
				events: []domain.Event{{
					Type:  domain.EvDelivered,
					Actor: p.ID,
					Msg:   &msg,
				}},
			}, nil
		case domain.OriginBot, domain.OriginAgent:
			// Outbound messages may only come from the current owner.
			if msg.Origin == domain.OriginAgent {
				if st.sess.Status != domain.StatusAssigned ||
					st.sess.Handler.AgentID != domain.AgentID(p.ID) {
					if p.Role != auth.RoleAdmin && p.Role != auth.RoleSystem {
						return mutateOutput{}, domain.ErrUnauthorized
					}
				}
			}
			if msg.Origin == domain.OriginBot && p.Role != auth.RoleSystem && p.Role != auth.RoleAdmin {
				return mutateOutput{}, domain.ErrUnauthorized
			}
			// Persist the reply and immediately ack it: it is not routed
			// back into a handler inbox.
			return mutateOutput{
				events: []domain.Event{
					{Type: domain.EvDelivered, Actor: p.ID, Msg: &msg},
					{Type: domain.EvAcked, Actor: p.ID, Msg: &domain.Message{ID: msg.ID}},
				},
			}, nil
		default:
			return mutateOutput{}, domain.ErrInvalidArgument
		}
	})
}

// Ack acknowledges one delivered message. Ack is idempotent.
func (e *Engine) Ack(ctx context.Context, id domain.SessionID, msgID string) error {
	if id == "" || msgID == "" {
		return domain.ErrInvalidArgument
	}
	return e.mutate(ctx, id, "handler", "session.ack", func(st *sessionState, now time.Time) (mutateOutput, error) {
		if !st.exists {
			return mutateOutput{}, domain.ErrSessionNotFound
		}
		if _, ok := st.pending[msgID]; !ok {
			// Unknown or already acked: treat as idempotent success
			// (duplicate ack must never double-process).
			return mutateOutput{}, nil
		}
		return mutateOutput{
			events: []domain.Event{{
				Type:  domain.EvAcked,
				Actor: "handler",
				Msg:   &domain.Message{ID: msgID},
			}},
		}, nil
	})
}
