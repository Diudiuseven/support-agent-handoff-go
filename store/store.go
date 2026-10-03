// Package store provides append-only event persistence used by the engine.
// Replaying the log reconstructs session state, which is how restart
// recovery is implemented.
//
// Seq convention: the first event of a session has Seq == 0, and an
// Append against a session that does not exist yet must pass
// expectedLastSeq == -1. Every later Append must pass the Seq of the
// last event already persisted; a mismatch returns domain.ErrConflict.
package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// EventStore is an ordered, durable log of events keyed by session.
type EventStore interface {
	// Append persists events for one session atomically. All events must
	// have strictly increasing, contiguous Seq values starting at
	// expectedLastSeq+1. It returns domain.ErrConflict on a mismatch.
	Append(ctx context.Context, sessionID domain.SessionID, expectedLastSeq domain.EventSeq, events ...domain.Event) error
	// ReadSession returns all events of one session in seq order.
	ReadSession(ctx context.Context, sessionID domain.SessionID) ([]domain.Event, error)
	// ReadAll returns all events across sessions (ordered by session id
	// then seq), used for crash recovery.
	ReadAll(ctx context.Context) ([]domain.Event, error)
	// Close flushes and releases resources.
	Close() error
}

// validateAppend checks the optimistic-seq and contiguity rules shared
// by every implementation.
func validateAppend(existing int, expectedLastSeq domain.EventSeq, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	if domain.EventSeq(existing-1) != expectedLastSeq {
		return domain.ErrConflict
	}
	want := expectedLastSeq + 1
	for i := range events {
		if events[i].Seq != want {
			return errors.New("store: non-contiguous event sequence")
		}
		want++
	}
	return nil
}

// MemoryStore is an in-process EventStore (tests / ephemeral runs).
type MemoryStore struct {
	mu     sync.Mutex
	events map[domain.SessionID][]domain.Event
}

// NewMemoryStore constructs an empty memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{events: make(map[domain.SessionID][]domain.Event)}
}

// Append implements EventStore.
func (s *MemoryStore) Append(_ context.Context, sessionID domain.SessionID, expectedLastSeq domain.EventSeq, events ...domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	have := s.events[sessionID]
	if err := validateAppend(len(have), expectedLastSeq, events); err != nil {
		return err
	}
	s.events[sessionID] = append(have, events...)
	return nil
}

// ReadSession implements EventStore.
func (s *MemoryStore) ReadSession(_ context.Context, sessionID domain.SessionID) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.events[sessionID]
	if len(src) == 0 {
		return nil, domain.ErrSessionNotFound
	}
	out := make([]domain.Event, len(src))
	copy(out, src)
	return out, nil
}

// ReadAll implements EventStore.
func (s *MemoryStore) ReadAll(_ context.Context) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.events))
	for id := range s.events {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	var out []domain.Event
	for _, id := range ids {
		out = append(out, s.events[domain.SessionID(id)]...)
	}
	return out, nil
}

// Close implements EventStore.
func (s *MemoryStore) Close() error { return nil }

// FileStore persists events as newline-delimited JSON files under a
// directory (one file per session). Each Append batch is serialized,
// written in one syscall and fsynced before success is reported.
type FileStore struct {
	dir string

	mu    sync.Mutex
	locks map[domain.SessionID]*sync.Mutex
	count map[domain.SessionID]int
}

// NewFileStore opens (creating if needed) the store at dir.
func NewFileStore(_ context.Context, dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("store: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir, locks: make(map[domain.SessionID]*sync.Mutex), count: make(map[domain.SessionID]int)}, nil
}

func (s *FileStore) path(id domain.SessionID) string {
	// Session ids come from external callers; hex-encode so the id can
	// never escape the data directory or collide on path separators.
	name := hex.EncodeToString([]byte(id)) + ".log"
	return filepath.Join(s.dir, name)
}

func (s *FileStore) lockFor(id domain.SessionID) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[id]
	if !ok {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	return m
}

func decodeFile(path string) ([]domain.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []domain.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e domain.Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// Append implements EventStore.
func (s *FileStore) Append(_ context.Context, sessionID domain.SessionID, expectedLastSeq domain.EventSeq, events ...domain.Event) error {
	mu := s.lockFor(sessionID)
	mu.Lock()
	defer mu.Unlock()

	path := s.path(sessionID)
	var existing []domain.Event
	if _, err := os.Stat(path); err == nil {
		existing, err = decodeFile(path)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := validateAppend(len(existing), expectedLastSeq, events); err != nil {
		return err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range events {
		if err := enc.Encode(&events[i]); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// fsync the directory so the file's very first creation is durable.
	if len(existing) == 0 {
		if d, derr := os.Open(s.dir); derr == nil {
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}

// ReadSession implements EventStore.
func (s *FileStore) ReadSession(_ context.Context, sessionID domain.SessionID) ([]domain.Event, error) {
	events, err := decodeFile(s.path(sessionID))
	if os.IsNotExist(err) {
		return nil, domain.ErrSessionNotFound
	}
	return events, err
}

// ReadAll implements EventStore.
func (s *FileStore) ReadAll(_ context.Context) ([]domain.Event, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []domain.Event
	for _, p := range files {
		events, err := decodeFile(p)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	return out, nil
}

// Close implements EventStore.
func (s *FileStore) Close() error { return nil }
