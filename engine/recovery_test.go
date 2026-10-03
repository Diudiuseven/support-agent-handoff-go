package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"
)

// rebuildEngine creates a fresh engine sharing the same durable store
// and runs recovery, simulating a process restart.
func rebuildEngine(t *testing.T, h *harness, maxWait time.Duration) *Engine {
	t.Helper()
	q2 := queue.NewFIFO()
	aud := audit.NewLogger(h.memAud)
	eng2 := New(h.eng.store, h.reg, q2, auth.NewRBACPolicy(), aud, map[string]Inbox{
		InboxBot:               h.bot,
		InboxAgentPrefix + "A": h.agA,
		InboxAgentPrefix + "B": h.agB,
	}, Options{QueueMaxWait: maxWait, SweepInterval: 50 * time.Millisecond})
	if err := eng2.Recover(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	return eng2
}

// TestRecoverAssignedSession_RedeliverOnce: assigned session with
// unacked messages is resumed; messages are redelivered exactly once
// (idempotent ack) and ownership/capacity are restored.
func TestRecoverAssignedSession_RedeliverOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A")
	sid := domain.SessionID("rec-1")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "r1", SessionID: sid, Origin: domain.OriginCustomer})
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "r2", SessionID: sid, Origin: domain.OriginCustomer})
	if ds := drain(h.agA.C, 2, time.Second); len(ds) != 2 {
		t.Fatalf("initial deliveries = %d", len(ds))
	}
	// Crash before ack.
	eng2 := rebuildEngine(t, h, 0)
	sess, ok := eng2.Session(sid)
	if !ok || sess.Status != domain.StatusAssigned || sess.Handler.AgentID != "A" {
		t.Fatalf("recovered session = %+v", sess)
	}
	snap, _ := h.reg.Get("A")
	if snap.Load != 1 {
		t.Fatalf("capacity not recovered, load=%d", snap.Load)
	}
	got := drain(h.agA.C, 2, time.Second)
	if len(got) != 2 || got[0].Msg.ID != "r1" || got[1].Msg.ID != "r2" {
		t.Fatalf("redelivery order = %+v", got)
	}
	for _, d := range got {
		if !d.Redelivered {
			t.Fatalf("expected Redelivered on %s", d.Msg.ID)
		}
	}
	// Ack once; no further redeliveries exist (pending cleared).
	for _, d := range got {
		if err := eng2.Ack(ctx, sid, d.Msg.ID); err != nil {
			t.Fatal(err)
		}
	}
	if len(eng2.Pending(sid)) != 0 {
		t.Fatalf("pending after ack = %d", len(eng2.Pending(sid)))
	}
	// Idempotent double ack must not create duplicate events beyond one
	// ack per message.
	_ = eng2.Ack(ctx, sid, "r1")
	evs, _ := h.eng.store.ReadSession(ctx, sid)
	ackCount := 0
	for _, e := range evs {
		if e.Type == domain.EvAcked && e.Msg != nil && e.Msg.ID == "r1" {
			ackCount++
		}
	}
	if ackCount != 1 {
		t.Fatalf("r1 acked %d times, want 1", ackCount)
	}
}

// TestRecoverQueuedSession: queued sessions reappear in the queue and
// are dispatched once an agent is online.
func TestRecoverQueuedSession(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, time.Minute)
	// Agent registered but offline -> queue.
	_ = h.reg.Register(regProfile("A", 1))
	sid := domain.SessionID("rec-q")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	eng2 := rebuildEngine(t, h, time.Minute)
	if eng2.q.Len("support") != 1 {
		t.Fatalf("queue after recovery len=%d", eng2.q.Len("support"))
	}
	// Bring A online and dispatch.
	h.reg.SetOnline("A", true)
	eng2.dispatchQueued(ctx, time.Now())
	sess, _ := eng2.Session(sid)
	if sess.Status != domain.StatusAssigned || sess.Handler.AgentID != "A" {
		t.Fatalf("queued session not assigned after recovery, %+v", sess)
	}
}

// TestRecoverMidTransfer: a transferring session survives restart; the
// target can accept and receive unacked messages in order.
func TestRecoverMidTransfer(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("rec-tr")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "t1", SessionID: sid, Origin: domain.OriginCustomer})
	_ = drain(h.agA.C, 1, time.Second)
	if err := h.eng.TransferTo(ctx, agentP("A"), sid, "B", time.Now()); err != nil {
		t.Fatal(err)
	}
	eng2 := rebuildEngine(t, h, 0)
	sess, _ := eng2.Session(sid)
	if sess.Status != domain.StatusTransferring || sess.TransferTo != "B" {
		t.Fatalf("transfer not recovered: %+v", sess)
	}
	// Both owner and target reservations restored without double count.
	if a, _ := h.reg.Get("A"); a.Load != 1 {
		t.Fatalf("A load=%d", a.Load)
	}
	if b, _ := h.reg.Get("B"); b.Load != 1 {
		t.Fatalf("B load=%d", b.Load)
	}
	if err := eng2.Accept(ctx, agentP("B"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	got := drain(h.agB.C, 1, time.Second)
	if len(got) != 1 || got[0].Msg.ID != "t1" {
		t.Fatalf("post-recovery transfer delivery = %+v", got)
	}
	// A's reservation released at accept.
	if a, _ := h.reg.Get("A"); a.Load != 0 {
		t.Fatalf("A load after accept=%d", a.Load)
	}
}

// TestRecoverAssignedButAgentOffline: assigned session whose agent is
// offline at restart is safely returned to the bot.
func TestRecoverAssignedButAgentOffline(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A")
	sid := domain.SessionID("rec-off")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	h.reg.SetOnline("A", false)

	eng2 := rebuildEngine(t, h, 0)
	sess, _ := eng2.Session(sid)
	if sess.Status != domain.StatusBot {
		t.Fatalf("want bot fallback after recovery, got %s", sess.Status)
	}
}

var _ = agent.Profile{}
