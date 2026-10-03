// Package domain contains the core types of the customer-service handoff
// system: sessions, messages, handlers and the append-only events that
// describe every state transition.
package domain

import "time"

// SessionID uniquely identifies a conversation with one customer.
type SessionID string

// AgentID identifies a human agent account.
type AgentID string

// QueueName identifies a skill-based routing queue.
type QueueName string

// EventSeq is the zero-based sequence number of an event within a session.
type EventSeq int64

// HandlerKind distinguishes the two kinds of session processors.
type HandlerKind string

const (
	HandlerBot   HandlerKind = "bot"
	HandlerAgent HandlerKind = "agent"
)

// SessionStatus is the high-level state of a session.
type SessionStatus string

const (
	// StatusBot: the bot owns the session.
	StatusBot SessionStatus = "bot"
	// StatusQueued: waiting for a human agent.
	StatusQueued SessionStatus = "queued"
	// StatusAssigned: owned by exactly one human agent.
	StatusAssigned SessionStatus = "assigned"
	// StatusTransferring: transient, an agent-to-agent transfer is in flight.
	StatusTransferring SessionStatus = "transferring"
	// StatusClosed: the session has ended (no further transitions).
	StatusClosed SessionStatus = "closed"
)

// ReasonCode explains why a transition happened, in particular the
// distinguishable outcomes of queueing (timeout / fallback / upgrade).
type ReasonCode string

const (
	ReasonNone              ReasonCode = ""
	ReasonCustomerRequested ReasonCode = "customer_requested"
	ReasonBotEscalated      ReasonCode = "bot_escalated"
	ReasonQueueTimeout      ReasonCode = "queue_timeout"
	ReasonNoAgentAvailable  ReasonCode = "no_agent_available"
	ReasonAgentOffline      ReasonCode = "agent_offline"
	ReasonAgentTransferred  ReasonCode = "agent_transferred"
	ReasonAgentAccepted     ReasonCode = "agent_accepted"
	ReasonAgentRejected     ReasonCode = "agent_rejected"
	ReasonCustomerEnded     ReasonCode = "customer_ended"
	ReasonAgentEnded        ReasonCode = "agent_ended"
	ReasonAgentAtCapacity   ReasonCode = "agent_at_capacity"
	ReasonUnauthorized      ReasonCode = "unauthorized"
	ReasonRestartRecovered  ReasonCode = "restart_recovered"
)

// EventType enumerates every persisted state transition.
type EventType string

const (
	EvSessionOpened      EventType = "session_opened"
	EvEscalatedToQueue   EventType = "escalated_to_queue"
	EvAgentAssigned      EventType = "agent_assigned"
	EvAssignmentRejected EventType = "assignment_rejected"
	EvReturnedToBot      EventType = "returned_to_bot"
	EvTransferStarted    EventType = "transfer_started"
	EvTransferCompleted  EventType = "transfer_completed"
	EvTransferCanceled   EventType = "transfer_canceled"
	EvDelivered          EventType = "delivered"
	EvAcked              EventType = "acked"
	EvSessionClosed      EventType = "session_closed"
)

// Handler names the current processor of a session.
type Handler struct {
	Kind HandlerKind
	// AgentID is set only when Kind == HandlerAgent.
	AgentID AgentID
}

// IsBot reports whether the handler is the bot.
func (h Handler) IsBot() bool { return h.Kind == HandlerBot }

func (h Handler) String() string {
	if h.Kind == HandlerAgent {
		return "agent:" + string(h.AgentID)
	}
	return string(h.Kind)
}

// Origin marks where a message came from.
type MessageOrigin string

const (
	OriginCustomer MessageOrigin = "customer"
	OriginBot      MessageOrigin = "bot"
	OriginAgent    MessageOrigin = "agent"
	OriginSystem   MessageOrigin = "system"
)

// Message is a chat message routed to the current session handler.
type Message struct {
	ID        string
	SessionID SessionID
	Origin    MessageOrigin
	Sender    AgentID // used when Origin == OriginAgent
	Text      string
	At        time.Time
}

// Session is the materialized state of one conversation.
type Session struct {
	ID           SessionID
	Status       SessionStatus
	Handler      Handler
	Queue        QueueName
	WaitingSince time.Time
	// TransferTo is the target agent while Status == StatusTransferring.
	TransferTo AgentID
	// LastDelivered is the seq of the last event persisted for the session
	// (all events, not only deliveries).
	LastDelivered EventSeq
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// PendingDelivery is a message that was persisted but not yet acknowledged
// by its handler. After a restart these are redelivered exactly once.
type PendingDelivery struct {
	SessionID SessionID
	Msg       Message
	Handler   Handler
	EventSeq  EventSeq
}

// Event is an append-only, immutable fact. Events are the only thing that is
// persisted; Session state is derived by replaying them.
type Event struct {
	Seq       EventSeq
	SessionID SessionID
	Type      EventType
	At        time.Time
	Actor     string // id of the principal that triggered the event
	Reason    ReasonCode
	Msg       *Message
	AgentID   AgentID // assigned / transferred agent
	Queue     QueueName
}
