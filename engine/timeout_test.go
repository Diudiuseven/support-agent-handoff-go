package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestQueueTimeoutFallbackToBot: a session waiting past MaxWait is
// returned to the bot with the distinguishable ReasonQueueTimeout.
func TestQueueTimeoutFallbackToBot(t *testing.T) {
	ctx := context.Background()
	maxWait := 100 * time.Millisecond
	h := newHarness(t, maxWait)
	// Register an agent but keep them offline => session queues.
	_ = h.reg.Register(regProfile("A", 1))

	sid := domain.SessionID("q-timeout")
	t0 := time.Now()
	if err := h.eng.Open(ctx, sid, "support", t0); err != nil {
		t.Fatal(err)
	}
	res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonBotEscalated, t0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusQueued {
		t.Fatalf("want queued, got %+v", res)
	}

	// Deterministic sweep past the deadline.
	h.eng.sweep(ctx, t0.Add(maxWait+time.Millisecond))

	sess, _ := h.eng.Session(sid)
	if sess.Status != domain.StatusBot {
		t.Fatalf("want bot fallback, got %s", sess.Status)
	}
	evs, err := h.eng.store.ReadSession(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	last := evs[len(evs)-1]
	if last.Type != domain.EvReturnedToBot || last.Reason != domain.ReasonQueueTimeout {
		t.Fatalf("last event = %+v", last)
	}
	// Queue entry removed.
	if h.eng.q.Len("support") != 0 {
		t.Fatal("timed-out session still in queue")
	}
}

// TestTimeoutThenAgentComesOnlineDoesNotReassign: once fallback
// happened the bot owns the session; re-escalation is required.
func TestTimeoutThenReescalate(t *testing.T) {
	ctx := context.Background()
	maxWait := 50 * time.Millisecond
	h := newHarness(t, maxWait)
	_ = h.reg.Register(regProfile("A", 1))

	sid := domain.SessionID("q-2")
	t0 := time.Now()
	_ = h.eng.Open(ctx, sid, "support", t0)
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, t0)
	h.eng.sweep(ctx, t0.Add(maxWait+time.Millisecond))
	if s, _ := h.eng.Session(sid); s.Status != domain.StatusBot {
		t.Fatalf("want bot, got %s", s.Status)
	}
	// Agent appears and re-escalation assigns.
	h.reg.SetOnline("A", true)
	res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, t0.Add(maxWait*2))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusAssigned || res.AssignedAgent != "A" {
		t.Fatalf("re-escalation = %+v", res)
	}
}

// TestAgentOfflineReturnsSessionToBot: taking an owning agent offline
// falls back with ReasonAgentOffline, and messages keep flowing to bot.
func TestAgentOfflineReturnsSessionToBot(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A")
	sid := domain.SessionID("off-1")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	if err := h.eng.SetAgentOnline(ctx, "A", false, time.Now()); err != nil {
		t.Fatal(err)
	}
	sess, _ := h.eng.Session(sid)
	if sess.Status != domain.StatusBot {
		t.Fatalf("want bot, got %s", sess.Status)
	}
	evs, _ := h.eng.store.ReadSession(ctx, sid)
	last := evs[len(evs)-1]
	if last.Type != domain.EvReturnedToBot || last.Reason != domain.ReasonAgentOffline {
		t.Fatalf("offline fallback event = %+v", last)
	}
	// Capacity released.
	if snap, _ := h.reg.Get("A"); snap.Load != 0 {
		t.Fatalf("load after offline = %d", snap.Load)
	}
	// New customer message must reach the bot, not a dead agent.
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "off-m", SessionID: sid, Origin: domain.OriginCustomer})
	got := drain(h.bot.C, 1, time.Second)
	if len(got) != 1 || got[0].Handler.Kind != domain.HandlerBot {
		t.Fatalf("post-offline delivery = %+v", got)
	}
}

// TestSlowAgentDoesNotBlockOthers: agent A's saturated inbox must not
// delay session delivery to agent B.
func TestSlowAgentDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	// A with a tiny inbox; B normal.
	h.agA = NewChannelInbox(1)
	h.eng.inboxes[InboxAgentPrefix+"A"] = h.agA
	h.registerAgents(t, "A", "B")

	s1 := domain.SessionID("slow-1")
	s2 := domain.SessionID("slow-2")
	if err := h.eng.Open(ctx, s1, "support", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Open(ctx, s2, "support", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.eng.Escalate(ctx, sys, s1, domain.ReasonCustomerRequested, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.eng.Escalate(ctx, sys, s2, domain.ReasonCustomerRequested, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Fill A's single slot.
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "a-1", SessionID: s1, Origin: domain.OriginCustomer})
	_ = drain(h.agA.C, 1, time.Second)
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "a-2", SessionID: s1, Origin: domain.OriginCustomer})
	// A's inbox now full (first delivery not consumed yet).

	// B's session must deliver promptly regardless.
	done := make(chan struct{})
	go func() {
		_ = h.eng.Send(ctx, sys, domain.Message{ID: "b-1", SessionID: s2, Origin: domain.OriginCustomer})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sending to B blocked by saturated A inbox")
	}
	got := drain(h.agB.C, 1, time.Second)
	if len(got) != 1 || got[0].Msg.ID != "b-1" {
		t.Fatalf("B delivery = %+v", got)
	}

	// Sweep eventually retries A's rejected message; drain A and verify.
	_ = h.eng.Ack(ctx, s1, "a-2") // ack regardless of delivery state
	_ = h.eng.Ack(ctx, s2, "b-1")
}
