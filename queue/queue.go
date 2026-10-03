// Package queue implements FIFO waiting queues per skill, wait-timeout
// handling and ordered removal. Queue state itself is not persisted; on
// restart the engine reconstructs it by replaying session events.
package queue

import (
	"container/list"
	"sync"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// Entry is one session waiting in a queue.
type Entry struct {
	SessionID  domain.SessionID
	Queue      domain.QueueName
	EnqueuedAt time.Time
	Deadline   time.Time
}

// TimedOut reports whether the entry's wait deadline has passed at now.
// A zero deadline means "no timeout" and never expires.
func (e Entry) TimedOut(now time.Time) bool {
	return !e.Deadline.IsZero() && !now.Before(e.Deadline)
}

// Policy decides what happens when queuing cannot end in a human pickup.
type Policy struct {
	// MaxWait is the longest a session may wait. Zero means no timeout.
	MaxWait time.Duration
	// OnTimeout is the reason applied when the wait deadline expires.
	OnTimeout domain.ReasonCode
}

// DefaultPolicy returns a reasonable default policy: after 2 minutes
// without a human pickup the session falls back to the bot.
func DefaultPolicy() Policy {
	return Policy{MaxWait: 2 * time.Minute, OnTimeout: domain.ReasonQueueTimeout}
}

// Waiter is an abstract waiting queue. Implementations are safe for
// concurrent use.
type Waiter interface {
	// Enqueue adds an entry. Re-enqueuing an existing session moves it
	// to the back (its deadline is refreshed from the new entry).
	Enqueue(e Entry) error
	// Remove drops one session (picked up, canceled or timed out).
	Remove(id domain.SessionID) (Entry, bool)
	// Get returns the entry without removing it.
	Get(id domain.SessionID) (Entry, bool)
	// DueTimeouts returns and removes entries whose deadline is <= now,
	// oldest first.
	DueTimeouts(now time.Time) []Entry
	// Head returns the oldest entry for a queue, or false.
	Head(q domain.QueueName) (Entry, bool)
	// Len returns the number of waiting sessions in one queue.
	Len(q domain.QueueName) int
	// All returns a snapshot of every entry in enqueue order.
	All() []Entry
}

// FIFOQueue is the default in-memory Waiter. Order is global FIFO; the
// Head(q) filter gives per-queue FIFO, which keeps dispatch
// deterministic across queues.
type FIFOQueue struct {
	mu    sync.Mutex
	order *list.List // *Entry, oldest first
	index map[domain.SessionID]*list.Element
}

// NewFIFO constructs an empty queue set.
func NewFIFO() *FIFOQueue {
	return &FIFOQueue{order: list.New(), index: make(map[domain.SessionID]*list.Element)}
}

// Enqueue implements Waiter.
func (q *FIFOQueue) Enqueue(e Entry) error {
	if e.SessionID == "" {
		return domain.ErrInvalidArgument
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if el, ok := q.index[e.SessionID]; ok {
		q.order.Remove(el)
	}
	el := q.order.PushBack(&e)
	q.index[e.SessionID] = el
	return nil
}

// Remove implements Waiter.
func (q *FIFOQueue) Remove(id domain.SessionID) (Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	el, ok := q.index[id]
	if !ok {
		return Entry{}, false
	}
	q.order.Remove(el)
	delete(q.index, id)
	return *el.Value.(*Entry), true
}

// Get implements Waiter.
func (q *FIFOQueue) Get(id domain.SessionID) (Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	el, ok := q.index[id]
	if !ok {
		return Entry{}, false
	}
	return *el.Value.(*Entry), true
}

// DueTimeouts implements Waiter: expired entries are removed and
// returned oldest-first.
func (q *FIFOQueue) DueTimeouts(now time.Time) []Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []Entry
	for el := q.order.Front(); el != nil; {
		e := el.Value.(*Entry)
		next := el.Next()
		if e.TimedOut(now) {
			q.order.Remove(el)
			delete(q.index, e.SessionID)
			out = append(out, *e)
		}
		el = next
	}
	return out
}

// Head implements Waiter: the oldest still-waiting entry of queue q.
func (q *FIFOQueue) Head(qn domain.QueueName) (Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for el := q.order.Front(); el != nil; el = el.Next() {
		e := el.Value.(*Entry)
		if e.Queue == qn {
			return *e, true
		}
	}
	return Entry{}, false
}

// Len implements Waiter.
func (q *FIFOQueue) Len(qn domain.QueueName) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for el := q.order.Front(); el != nil; el = el.Next() {
		if el.Value.(*Entry).Queue == qn {
			n++
		}
	}
	return n
}

// All implements Waiter.
func (q *FIFOQueue) All() []Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Entry, 0, q.order.Len())
	for el := q.order.Front(); el != nil; el = el.Next() {
		out = append(out, *el.Value.(*Entry))
	}
	return out
}
