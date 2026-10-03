package audit

import (
	"context"
	"testing"
)

func TestMemorySinkScanFilter(t *testing.T) {
	ctx := context.Background()
	s := NewMemorySink()
	l := NewLogger(s)
	_ = l.Log(ctx, Record{Actor: "A", Action: "take", Session: "s1", Outcome: OutcomeAllowed})
	_ = l.Log(ctx, Record{Actor: "B", Action: "take", Session: "s2", Outcome: OutcomeDenied})
	if n := s.Len(); n != 2 {
		t.Fatalf("len=%d", n)
	}
	got, _ := s.Scan(ctx, "s1")
	if len(got) != 1 || got[0].Session != "s1" {
		t.Fatalf("filter = %+v", got)
	}
	if err := l.Denied(ctx, "C", "take", "A", "unauthorized"); err != nil {
		t.Fatal(err)
	}
	all, _ := s.Scan(ctx, "")
	if len(all) != 3 || all[2].Outcome != OutcomeDenied {
		t.Fatalf("denied record = %+v", all)
	}
}

func TestNilLoggerSafe(t *testing.T) {
	var l *Logger
	if err := l.Log(context.Background(), Record{}); err != nil {
		t.Fatal(err)
	}
}
