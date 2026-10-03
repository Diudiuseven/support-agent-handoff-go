package engine

import "time"

// Options configures an Engine.
type Options struct {
	// QueueMaxWait is the default maximum queue wait. Zero disables
	// timeout-based fallback; otherwise a session still waiting when the
	// deadline elapses is returned to the bot with
	// ReasonQueueTimeout.
	QueueMaxWait time.Duration

	// DeliveryBuffer is the default per-handler outbound buffer size
	// applied to inboxes constructed via NewDefaultInboxes. Handlers
	// that supply their own Inbox choose their own capacity.
	DeliveryBuffer int

	// SweepInterval is how often the background loop checks queue
	// timeouts, re-dispatches queued sessions and retries deliveries
	// rejected by saturated inboxes.
	SweepInterval time.Duration
}

// DefaultOptions returns sane defaults.
func DefaultOptions() Options {
	return Options{
		QueueMaxWait:   2 * time.Minute,
		DeliveryBuffer: 64,
		SweepInterval:  time.Second,
	}
}
