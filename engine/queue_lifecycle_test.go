package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestCloseWhileQueued: closing a waiting session removes it from the
// queue and leaves no dangling state.
func TestCloseWhileQueued(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, time.Minute)
	// Only registered, offline => queue.
	_ = h.reg.Register(regProfile("A", 1))
	sid := domain.SessionID("q-close")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	if res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now()); err != nil || res.Status != domain.StatusQueued {
		t.Fatalf("escalate = %+v err=%v", res, err)
	}
	if err := h.eng.Close(ctx, admin, sid, domain.ReasonCustomerEnded, time.Now()); err != nil {
		t.Fatal(err)
	}
	if h.eng.q.Len("support") != 0 {
		t.Fatal("queue entry survived close")
	}
	if s, _ := h.eng.Session(sid); s.Status != domain.StatusClosed {
		t.Fatalf("status = %s", s.Status)
	}
	// Sweep must not resurrect or fallback a closed session.
	h.eng.sweep(ctx, time.Now().Add(time.Hour))
	if s, _ := h.eng.Session(sid); s.Status != domain.StatusClosed {
		t.Fatalf("sweep mutated closed session to %s", s.Status)
	}
}

// TestEscalateIdempotentWhileQueuedAndRejectWhenAssigned.
func TestEscalateStateGuards(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A")
	sid := domain.SessionID("q-guard")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	// Already assigned to A: second escalate is rejected.
	if _, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now()); err != domain.ErrAlreadyAssigned {
		t.Fatalf("double escalate err=%v", err)
	}
	// Operations on unknown session.
	if _, err := h.eng.Escalate(ctx, sys, "nope", domain.ReasonCustomerRequested, time.Now()); err != domain.ErrSessionNotFound {
		t.Fatalf("missing session err=%v", err)
	}
	if err := h.eng.Send(ctx, sys, domain.Message{ID: "m", SessionID: "m", Origin: domain.OriginCustomer}); err != domain.ErrSessionNotFound {
		t.Fatalf("send to missing err=%v", err)
	}
}

// TestMessageSendToClosedSessionRejected.
func TestMessageSendToClosedSessionRejected(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	sid := domain.SessionID("closed-send")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_ = h.eng.Close(ctx, sys, sid, domain.ReasonCustomerEnded, time.Now())
	err := h.eng.Send(ctx, sys, domain.Message{ID: "m", SessionID: sid, Origin: domain.OriginCustomer})
	if err != domain.ErrSessionClosed {
		t.Fatalf("send to closed err=%v", err)
	}
}

// TestQueueFIFOFairness: with one agent freed later, the oldest queued
// session wins deterministically.
func TestQueueFIFOFairness(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	_ = h.reg.Register(regProfile("A", 1)) // offline initially
	t0 := time.Now()
	var sids []domain.SessionID
	for i := 0; i < 5; i++ {
		sid := domain.SessionID(domainSess(2000 + i))
		sids = append(sids, sid)
		_ = h.eng.Open(ctx, sid, "support", t0.Add(time.Duration(i)*time.Millisecond))
		if res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, t0.Add(time.Duration(i)*time.Millisecond)); err != nil || res.Status != domain.StatusQueued {
			t.Fatalf("escalate %s: %+v %v", sid, res, err)
		}
	}
	h.reg.SetOnline("A", true)
	h.eng.dispatchQueued(ctx, t0.Add(time.Second))
	s, _ := h.eng.Session(sids[0])
	if s.Status != domain.StatusAssigned || s.Handler.AgentID != "A" {
		t.Fatalf("oldest not picked first: %+v", s)
	}
	// The rest remain queued, still in order.
	for i := 1; i < 5; i++ {
		if s, _ := h.eng.Session(sids[i]); s.Status != domain.StatusQueued {
			t.Fatalf("session %d status=%s", i, s.Status)
		}
	}
	head, ok := h.eng.q.Head("support")
	if !ok || head.SessionID != sids[1] {
		t.Fatalf("next head = %+v", head)
	}
}
