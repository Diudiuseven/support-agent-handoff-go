package queue

import (
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func TestFIFOOrderingAndHead(t *testing.T) {
	q := NewFIFO()
	now := time.Now()
	entries := []Entry{
		{SessionID: "1", Queue: "q1", EnqueuedAt: now},
		{SessionID: "2", Queue: "q2", EnqueuedAt: now.Add(time.Millisecond)},
		{SessionID: "3", Queue: "q1", EnqueuedAt: now.Add(2 * time.Millisecond)},
	}
	for _, e := range entries {
		if err := q.Enqueue(e); err != nil {
			t.Fatal(err)
		}
	}
	if h, ok := q.Head("q1"); !ok || h.SessionID != "1" {
		t.Fatalf("head q1 = %+v %v", h, ok)
	}
	if q.Len("q1") != 2 || q.Len("q2") != 1 {
		t.Fatal("len wrong")
	}
	if e, ok := q.Remove("1"); !ok || e.SessionID != "1" {
		t.Fatal("remove failed")
	}
	if h, _ := q.Head("q1"); h.SessionID != "3" {
		t.Fatalf("head after remove = %v", h.SessionID)
	}
}

func TestFIFODueTimeouts(t *testing.T) {
	q := NewFIFO()
	t0 := time.Now()
	_ = q.Enqueue(Entry{SessionID: "a", Queue: "x", Deadline: t0.Add(time.Second)})
	_ = q.Enqueue(Entry{SessionID: "b", Queue: "x", Deadline: t0.Add(-time.Second)})
	_ = q.Enqueue(Entry{SessionID: "c", Queue: "x"}) // no deadline
	due := q.DueTimeouts(t0)
	if len(due) != 1 || due[0].SessionID != "b" {
		t.Fatalf("due = %+v", due)
	}
	if q.Len("x") != 2 {
		t.Fatalf("remaining = %d", q.Len("x"))
	}
}

func TestFIFOReEnqueueMovesToBack(t *testing.T) {
	q := NewFIFO()
	_ = q.Enqueue(Entry{SessionID: "a", Queue: "x"})
	_ = q.Enqueue(Entry{SessionID: "b", Queue: "x"})
	_ = q.Enqueue(Entry{SessionID: "a", Queue: "x"})
	all := q.All()
	if len(all) != 2 || all[0].SessionID != "b" || all[1].SessionID != "a" {
		t.Fatalf("order = %+v", all)
	}
}

var _ = domain.SessionID("")
