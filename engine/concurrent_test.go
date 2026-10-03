package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestConcurrentTake_SingleWinner: N agents race to take one queued
// session. Exactly one must win; everyone else gets a classified error,
// and capacity accounting must show the session on exactly one agent.
func TestConcurrentTake_SingleWinner(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)

	const n = 16
	for i := range n {
		id := agentID(i)
		_ = h.reg.Register(regProfile(id, 5))
		h.reg.SetOnline(id, true)
	}
	// All agents registered, but force queueing by taking them offline
	// at escalation time.
	for i := range n {
		h.reg.SetOnline(agentID(i), false)
	}
	sid := domain.SessionID("race-1")
	if err := h.eng.Open(ctx, sid, "support", time.Now()); err != nil {
		t.Fatal(err)
	}
	res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusQueued {
		t.Fatalf("expected queued, got %+v", res)
	}
	// Bring everyone online and race.
	for i := range n {
		h.reg.SetOnline(agentID(i), true)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := map[string]int{}
	errClasses := map[string]int{}
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			err := h.eng.Take(ctx, agentP(string(agentID(i))), sid, time.Now())
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners[string(agentID(i))]++
			} else {
				errClasses[err.Error()]++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("expected exactly 1 winner, got %d: %v errs=%v", len(winners), winners, errClasses)
	}
	var winner string
	for k := range winners {
		winner = k
	}
	if winners[winner] != 1 {
		t.Fatalf("winner took session %d times", winners[winner])
	}
	sess, ok := h.eng.Session(sid)
	if !ok || sess.Handler.AgentID != domain.AgentID(winner) {
		t.Fatalf("owner mismatch: %+v winner=%s", sess, winner)
	}
	// Capacity: session counted on winner once and on nobody else.
	for i := range n {
		snap, _ := h.reg.Get(agentID(i))
		if string(agentID(i)) == winner {
			if snap.Load != 1 {
				t.Fatalf("winner load=%d", snap.Load)
			}
		} else if snap.Load != 0 {
			t.Fatalf("loser %s has load %d", agentID(i), snap.Load)
		}
	}
	// Queue must be empty.
	if h.eng.q.Len("support") != 0 {
		t.Fatalf("queue not drained: %d", h.eng.q.Len("support"))
	}
}

// TestAgentServesManySessionsWithoutInterference: one agent holding
// multiple concurrent sessions must not leak capacity or messages
// across them.
func TestAgentServesManySessionsWithoutInterference(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	_ = h.reg.Register(regProfile("A", 3))
	h.reg.SetOnline("A", true)

	const sessions = 3
	for i := range sessions {
		sid := domain.SessionID(domainSess(i))
		if err := h.eng.Open(ctx, sid, "support", time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := h.eng.Send(ctx, sys, domain.Message{
			ID: "msg-" + string(sid), SessionID: sid, Origin: domain.OriginCustomer,
		}); err != nil {
			t.Fatal(err)
		}
	}
	got := drain(h.agA.C, sessions, time.Second)
	if len(got) != sessions {
		t.Fatalf("got %d deliveries, want %d", len(got), sessions)
	}
	seen := map[domain.SessionID]bool{}
	for _, d := range got {
		if d.Msg.SessionID != d.SessionID || d.Msg.ID != "msg-"+string(d.SessionID) {
			t.Fatalf("message misrouted: %+v", d)
		}
		seen[d.SessionID] = true
	}
	if len(seen) != sessions {
		t.Fatalf("sessions seen = %v", seen)
	}
	snap, _ := h.reg.Get("A")
	if snap.Load != 3 {
		t.Fatalf("load = %d want 3", snap.Load)
	}
	// Fourth session must not be auto-assigned to A (at capacity) and
	// must queue instead.
	extra := domain.SessionID("extra")
	_ = h.eng.Open(ctx, extra, "support", time.Now())
	res, err := h.eng.Escalate(ctx, sys, extra, domain.ReasonCustomerRequested, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusQueued {
		t.Fatalf("capacity session should queue, got %+v", res)
	}
}

// TestConcurrentMixedTransitions: take / close / send racing on
// different sessions never corrupt state; every session ends in an
// explainable status.
func TestConcurrentMixedTransitions(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	_ = h.reg.Register(regProfile("A", 100))
	h.reg.SetOnline("A", true)

	const sessions = 30
	for i := range sessions {
		sid := domain.SessionID(domainSess(100 + i))
		if err := h.eng.Open(ctx, sid, "support", time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range sessions {
		sid := domain.SessionID(domainSess(100 + i))
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
		}()
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 5; j++ {
				_ = h.eng.Send(ctx, sys, domain.Message{
					ID: string(domainSess(900000 + i*10 + j)), SessionID: sid, Origin: domain.OriginCustomer,
				})
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			time.Sleep(time.Duration(i%5) * time.Millisecond)
			_ = h.eng.Close(ctx, admin, sid, domain.ReasonCustomerEnded, time.Now())
		}()
	}
	close(start)
	wg.Wait()

	for i := range sessions {
		sid := domain.SessionID(domainSess(100 + i))
		sess, ok := h.eng.Session(sid)
		if !ok {
			t.Fatal("session missing")
		}
		if sess.Status != domain.StatusClosed && sess.Status != domain.StatusAssigned {
			t.Fatalf("session %s unexpected status %s", sid, sess.Status)
		}
		if sess.Status == domain.StatusClosed && errors.Is(nil, nil) {
			if sess.Handler.Kind == domain.HandlerAgent {
				// closed sessions release capacity — sanity checked below
			}
		}
	}
	snap, _ := h.reg.Get("A")
	// Load must equal number of still-assigned sessions.
	assigned := 0
	for i := range sessions {
		sid := domain.SessionID(domainSess(100 + i))
		if s, _ := h.eng.Session(sid); s.Status == domain.StatusAssigned {
			assigned++
		}
	}
	if snap.Load != assigned {
		t.Fatalf("capacity leak: registry load=%d but assigned=%d", snap.Load, assigned)
	}
}
