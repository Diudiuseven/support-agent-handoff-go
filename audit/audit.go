// Package audit records an append-only trail of every sensitive
// operation: authorized actions, denied attempts and state transitions.
package audit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

// Outcome of an audited action.
type Outcome string

const (
	OutcomeAllowed Outcome = "allowed"
	OutcomeDenied  Outcome = "denied"
	OutcomeIgnored Outcome = "ignored" // e.g. duplicate/no-op
)

// Record is one audit entry.
type Record struct {
	Time    time.Time `json:"time"`
	Actor   string    `json:"actor"`
	Action  string    `json:"action"`
	Session string    `json:"session,omitempty"`
	Target  string    `json:"target,omitempty"` // target agent id, if any
	Outcome Outcome   `json:"outcome"`
	Reason  string    `json:"reason,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

// Sink persists or exports audit records.
type Sink interface {
	Write(ctx context.Context, r Record) error
	// Scan returns records ordered oldest-first, optionally filtered by
	// session id (empty means all).
	Scan(ctx context.Context, session string) ([]Record, error)
}

// Logger coordinates writing records to one or more sinks.
type Logger struct {
	sinks []Sink
}

// NewLogger builds a logger writing to all given sinks. A nil-safe
// logger (no sinks) simply discards records.
func NewLogger(sinks ...Sink) *Logger {
	filtered := make([]Sink, 0, len(sinks))
	for _, s := range sinks {
		if s != nil {
			filtered = append(filtered, s)
		}
	}
	return &Logger{sinks: filtered}
}

// Log writes a record to every sink. A failure of one sink does not
// stop delivery to the others; errors are joined.
func (l *Logger) Log(ctx context.Context, r Record) error {
	if l == nil {
		return nil
	}
	if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	}
	var errs []error
	for _, s := range l.sinks {
		if err := s.Write(ctx, r); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Denied is a convenience for logging a permission failure.
func (l *Logger) Denied(ctx context.Context, actor, action, target, reason string) error {
	return l.Log(ctx, Record{
		Actor:   actor,
		Action:  action,
		Target:  target,
		Outcome: OutcomeDenied,
		Reason:  reason,
	})
}

// MemorySink keeps records in memory.
type MemorySink struct {
	mu      sync.Mutex
	records []Record
}

// NewMemorySink constructs an empty memory sink.
func NewMemorySink() *MemorySink { return &MemorySink{} }

// Write implements Sink.
func (s *MemorySink) Write(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
	return nil
}

// Scan implements Sink.
func (s *MemorySink) Scan(_ context.Context, session string) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.records))
	for _, r := range s.records {
		if session == "" || r.Session == session {
			out = append(out, r)
		}
	}
	return out, nil
}

// Len returns the number of stored records.
func (s *MemorySink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// FileSink appends JSON lines to a file with fsync per record.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
}

// NewFileSink opens (or creates) the audit file at path.
func NewFileSink(_ context.Context, path string) (*FileSink, error) {
	if path == "" {
		return nil, errors.New("audit: empty path")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

// Write implements Sink.
func (s *FileSink) Write(_ context.Context, r Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(line); err != nil {
		return err
	}
	return s.f.Sync()
}

// Scan implements Sink by re-reading the file from the beginning.
func (s *FileSink) Scan(_ context.Context, session string) ([]Record, error) {
	s.mu.Lock()
	path := s.f.Name()
	s.mu.Unlock()

	var out []Record
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, err
		}
		if session == "" || r.Session == session {
			out = append(out, r)
		}
	}
	return out, sc.Err()
}

// Close flushes and closes the file.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
