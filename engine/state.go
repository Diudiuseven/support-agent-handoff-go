package engine

import (
	"sync"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// sessionState is the engine's live view of one session. All access
// goes through mu, which is the single serialization point for the
// session: every transition, message send and ack on the same session
// is totally ordered, while different sessions use different locks so
// a slow agent on one never blocks another.
type sessionState struct {
	mu sync.Mutex

	sess   domain.Session
	exists bool

	// pending holds delivered-but-unacked inbound messages in arrival
	// (seq) order. pendingOrder preserves FIFO iteration.
	pending      map[string]domain.PendingDelivery
	pendingOrder []string
	// inflight marks messages currently pushed to a handler but not yet
	// acked, so the sweep does not push duplicates unless the push
	// itself failed.
	inflight map[string]bool
	// redelivered marks messages that are replays (crash recovery or
	// ownership change) so handlers receive the Redelivered flag.
	redelivered map[string]bool
}

func newSessionState() *sessionState {
	return &sessionState{
		pending:     make(map[string]domain.PendingDelivery),
		inflight:    make(map[string]bool),
		redelivered: make(map[string]bool),
	}
}

// rebindPendingLocked moves all unacked messages to the session's
// current handler and schedules re-delivery (ownership changed or the
// process restarted). Caller holds mu.
func (st *sessionState) rebindPendingLocked() {
	h := st.sess.Handler
	for _, id := range st.pendingOrder {
		if p, ok := st.pending[id]; ok {
			if !sameHandler(p.Handler, h) {
				p.Handler = h
				st.pending[id] = p
				st.redelivered[id] = true
			}
			st.inflight[id] = false
		}
	}
}

// snapshot copies the session header under the caller's locking
// discipline (caller must hold mu).
func (st *sessionState) snapshotLocked() domain.Session {
	cp := st.sess
	return cp
}

func (st *sessionState) pendingListLocked() []domain.PendingDelivery {
	out := make([]domain.PendingDelivery, 0, len(st.pendingOrder))
	for _, id := range st.pendingOrder {
		if p, ok := st.pending[id]; ok {
			out = append(out, p)
		}
	}
	return out
}

// materialize replays one session's event log into fresh live state.
// It is the single source of truth for the projection: both restart
// recovery and (conceptually) runtime transitions follow the same
// rules. Events are assumed contiguous and already validated by the
// store.
func materialize(sessionID domain.SessionID, events []domain.Event) *sessionState {
	st := newSessionState()
	for _, ev := range events {
		st.applyLocked(ev)
	}
	// Reconcile pendingOrder with the map (acks delete entries). After
	// a restart every still-pending message is a replay, so it is marked
	// redelivered and not in-flight; the engine pushes it again once.
	if len(st.pendingOrder) > 0 {
		kept := st.pendingOrder[:0]
		for _, id := range st.pendingOrder {
			if _, ok := st.pending[id]; ok {
				kept = append(kept, id)
				st.redelivered[id] = true
				st.inflight[id] = false
			} else {
				delete(st.inflight, id)
				delete(st.redelivered, id)
			}
		}
		st.pendingOrder = kept
	}
	return st
}

// applyLocked folds one event into the projection. The caller (replay
// or the transition code) is responsible for locking.
func (st *sessionState) applyLocked(ev domain.Event) {
	st.sess.ID = ev.SessionID
	st.sess.LastDelivered = ev.Seq
	st.sess.UpdatedAt = ev.At

	switch ev.Type {
	case domain.EvSessionOpened:
		st.exists = true
		st.sess.CreatedAt = ev.At
		st.sess.Status = domain.StatusBot
		st.sess.Handler = domain.Handler{Kind: domain.HandlerBot}
		st.sess.Queue = ev.Queue
	case domain.EvEscalatedToQueue:
		st.sess.Status = domain.StatusQueued
		st.sess.Queue = ev.Queue
		st.sess.WaitingSince = ev.At
		st.sess.TransferTo = ""
	case domain.EvAgentAssigned:
		st.sess.Status = domain.StatusAssigned
		st.sess.Handler = domain.Handler{Kind: domain.HandlerAgent, AgentID: ev.AgentID}
		st.sess.Queue = ev.Queue
		st.sess.WaitingSince = timeZero()
		st.sess.TransferTo = ""
	case domain.EvReturnedToBot:
		st.sess.Status = domain.StatusBot
		st.sess.Handler = domain.Handler{Kind: domain.HandlerBot}
		st.sess.TransferTo = ""
	case domain.EvTransferStarted:
		st.sess.Status = domain.StatusTransferring
		st.sess.TransferTo = ev.AgentID
	case domain.EvTransferCompleted:
		st.sess.Status = domain.StatusAssigned
		st.sess.Handler = domain.Handler{Kind: domain.HandlerAgent, AgentID: ev.AgentID}
		st.sess.TransferTo = ""
	case domain.EvTransferCanceled:
		st.sess.Status = domain.StatusAssigned
		st.sess.TransferTo = ""
	case domain.EvSessionClosed:
		st.sess.Status = domain.StatusClosed
		st.sess.TransferTo = ""
	case domain.EvDelivered:
		if ev.Msg != nil {
			id := ev.Msg.ID
			if _, ok := st.pending[id]; !ok {
				st.pending[id] = domain.PendingDelivery{
					SessionID: ev.SessionID,
					Msg:       *ev.Msg,
					Handler:   st.sess.Handler,
					EventSeq:  ev.Seq,
				}
				st.pendingOrder = append(st.pendingOrder, id)
			}
		}
	case domain.EvAcked:
		if id := ackedMsgID(ev); id != "" {
			delete(st.pending, id)
			delete(st.inflight, id)
			delete(st.redelivered, id)
		}
	}
}
