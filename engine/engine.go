// Package engine orchestrates the handoff lifecycle: escalation,
// queueing, competitive pickup (take), agent-to-agent transfer,
// ordered message routing, persistence and restart recovery.
//
// Concurrency model:
//
//   - Each session has its own mutex (sessionState.mu). Every state
//     transition for a session is serialized on that mutex, while
//     different sessions never block each other.
//   - State transitions are persisted as one atomic event batch in the
//     EventStore using optimistic concurrency (expected last seq). If
//     the store rejects the batch (domain.ErrConflict) the engine
//     reloads that session and retries a bounded number of times, so
//     the outcome is always one of the well-defined transitions.
//   - Capacity is reserved in the agent registry inside the same
//     serialized critical section as the event append, giving a
//     deterministic single winner for competing takes.
//   - Deliveries to handler inboxes are non-blocking: a slow handler
//     applies backpressure only to its own sessions.
package engine

import (
	"context"
	"sync"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"
	"github.com/Diudiuseven/support-agent-handoff-go/store"
)

// Inbox keys passed to New's inboxes map.
const (
	// InboxBot receives deliveries while the bot owns a session.
	InboxBot = "bot"
	// InboxAgentPrefix + agentID receives deliveries for human agents.
	InboxAgentPrefix = "agent:"
)

// Delivery is a message handed to a handler for processing.
type Delivery struct {
	SessionID domain.SessionID
	Msg       domain.Message
	Handler   domain.Handler
	// Redelivered is true when this delivery is a replay after a crash,
	// an ownership change or an unacknowledged retry; handlers must
	// treat the message id as an idempotency key.
	Redelivered bool
}

// Inbox receives deliveries. Implementations must be safe for concurrent
// use; sends are non-blocking from the engine's perspective.
type Inbox interface {
	// TryDeliver attempts a non-blocking delivery. It returns false when
	// the inbox is temporarily unavailable (full); the engine then keeps
	// the message due and retries from the sweep loop.
	TryDeliver(d Delivery) bool
}

// InboxFunc adapts a function to an Inbox.
type InboxFunc func(Delivery)

// TryDeliver implements Inbox. The wrapped function is always assumed
// to accept the delivery.
func (f InboxFunc) TryDeliver(d Delivery) bool { f(d); return true }

// HandoffResult summarizes an escalation attempt.
type HandoffResult struct {
	SessionID     domain.SessionID
	Status        domain.SessionStatus
	AssignedAgent domain.AgentID
	Queue         domain.QueueName
	Reason        domain.ReasonCode
}

// Engine is the handoff coordinator. One Engine instance owns the whole
// routing state; it is safe for concurrent use.
type Engine struct {
	store   store.EventStore
	reg     agent.Registry
	q       queue.Waiter
	policy  auth.Policy
	aud     *audit.Logger
	inboxes map[string]Inbox
	opts    Options

	mu       sync.Mutex
	sessions map[domain.SessionID]*sessionState

	maxWait time.Duration

	stopCh     chan struct{}
	doneCh     chan struct{}
	runOnce    sync.Once
	stopOnce   sync.Once
	cancelLoop context.CancelFunc
	now        func() time.Time
}

// New constructs an Engine.
func New(es store.EventStore, reg agent.Registry, q queue.Waiter, pol auth.Policy, aud *audit.Logger, inboxes map[string]Inbox, opts Options) *Engine {
	if opts.DeliveryBuffer <= 0 {
		opts.DeliveryBuffer = 64
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = time.Second
	}
	return &Engine{
		store:    es,
		reg:      reg,
		q:        q,
		policy:   pol,
		aud:      aud,
		inboxes:  inboxes,
		opts:     opts,
		sessions: make(map[domain.SessionID]*sessionState),
		maxWait:  opts.QueueMaxWait,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// setNow overrides the clock (tests only).
func (e *Engine) setNow(f func() time.Time) {
	e.now = f
}

// getState returns the live state for a session.
func (e *Engine) getState(id domain.SessionID) (*sessionState, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.sessions[id]
	return st, ok
}

// getOrLoad returns live state, loading it from the store on cache miss.
func (e *Engine) getOrLoad(ctx context.Context, id domain.SessionID) (*sessionState, error) {
	if st, ok := e.getState(id); ok {
		return st, nil
	}
	events, err := e.store.ReadSession(ctx, id)
	if err != nil {
		return nil, err
	}
	st := materialize(id, events)
	e.mu.Lock()
	if existing, ok := e.sessions[id]; ok {
		st = existing
	} else {
		e.sessions[id] = st
	}
	e.mu.Unlock()
	return st, nil
}

// Session returns a snapshot of session state.
func (e *Engine) Session(id domain.SessionID) (domain.Session, bool) {
	st, ok := e.getState(id)
	if !ok {
		return domain.Session{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.snapshotLocked(), true
}

// Pending returns unacknowledged deliveries for a session.
func (e *Engine) Pending(id domain.SessionID) []domain.PendingDelivery {
	st, ok := e.getState(id)
	if !ok {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.pendingListLocked()
}

// inboxFor resolves the delivery target of a handler.
func (e *Engine) inboxFor(h domain.Handler) Inbox {
	if h.Kind == domain.HandlerAgent {
		if ib, ok := e.inboxes[InboxAgentPrefix+string(h.AgentID)]; ok {
			return ib
		}
		return nil
	}
	return e.inboxes[InboxBot]
}

// Stop shuts the background loop (if running) down and waits for it.
func (e *Engine) Stop() error {
	e.stopOnce.Do(func() { close(e.stopCh) })
	if e.cancelLoop != nil {
		e.cancelLoop()
	}
	select {
	case <-e.doneCh:
	default:
		// Loop never started: nothing to wait for.
	}
	return nil
}
