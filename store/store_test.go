package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func TestFileStore_AppendReadConflict(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := NewFileStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	sid := domain.SessionID("s-1")
	if err := fs.Append(ctx, sid, -1, domain.Event{Seq: 0, Type: domain.EvSessionOpened}); err != nil {
		t.Fatal(err)
	}
	// Duplicate create must conflict.
	if err := fs.Append(ctx, sid, -1, domain.Event{Seq: 0, Type: domain.EvSessionOpened}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	// Legit next append.
	if err := fs.Append(ctx, sid, 0, domain.Event{Seq: 1, Type: domain.EvDelivered}); err != nil {
		t.Fatalf("valid append failed: %v", err)
	}
	// Re-using the stale expected seq must conflict now.
	if err := fs.Append(ctx, sid, 0, domain.Event{Seq: 1, Type: domain.EvDelivered}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("want conflict on stale seq, got %v", err)
	}
	got, err := fs.ReadSession(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Seq != 1 {
		t.Fatalf("events = %+v", got)
	}
}

func TestFileStore_RestartAcrossHandles(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := NewFileStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	s1, s2 := domain.SessionID("s1"), domain.SessionID("s2")
	if err := fs.Append(ctx, s1, -1, domain.Event{Seq: 0, Type: domain.EvSessionOpened}); err != nil {
		t.Fatal(err)
	}
	if err := fs.Append(ctx, s2, -1, domain.Event{Seq: 0, Type: domain.EvSessionOpened}); err != nil {
		t.Fatal(err)
	}
	if err := fs.Append(ctx, s1, 0, domain.Event{Seq: 1, Type: domain.EvReturnedToBot}); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen.
	fs2, err := NewFileStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	all, err := fs2.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("reloaded %d events", len(all))
	}
	// Append must validate against the on-disk tail.
	if err := fs2.Append(ctx, s1, 1, domain.Event{Seq: 2, Type: domain.EvSessionClosed}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
}

func TestFileStore_SessionFileIsolated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, _ := NewFileStore(ctx, dir)
	if _, err := fs.ReadSession(ctx, "missing"); !errors.Is(err, domain.ErrSessionNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("failed read must not create files: %v", entries)
	}
}

func TestMemoryStore_Contiguity(t *testing.T) {
	ctx := context.Background()
	ms := NewMemoryStore()
	sid := domain.SessionID("x")
	if err := ms.Append(ctx, sid, -1, domain.Event{Seq: 0}); err != nil {
		t.Fatal(err)
	}
	if err := ms.Append(ctx, sid, 0, domain.Event{Seq: 5}); err == nil {
		t.Fatal("non-contiguous seq accepted")
	}
}
