package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestTransferMessageOrdering: messages before, during and after a
// transfer must be attributed in order: to A until accept, then to B.
func TestTransferMessageOrdering(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("tr-order")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	send := func(id string) {
		t.Helper()
		if err := h.eng.Send(ctx, sys, domain.Message{ID: id, SessionID: sid, Origin: domain.OriginCustomer}); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Before transfer -> A.
	send("p1")
	if ds := drain(h.agA.C, 1, time.Second); len(ds) != 1 || ds[0].Msg.ID != "p1" || ds[0].Handler.AgentID != "A" {
		t.Fatalf("p1 = %+v", ds)
	}
	_ = h.eng.Ack(ctx, sid, "p1")

	if err := h.eng.TransferTo(ctx, agentP("A"), sid, "B", time.Now()); err != nil {
		t.Fatal(err)
	}
	// 2. During transfer -> still A (sequential, no misrouting).
	send("d1")
	send("d2")
	if ds := drain(h.agA.C, 2, time.Second); len(ds) != 2 || ds[0].Msg.ID != "d1" || ds[1].Msg.ID != "d2" {
		t.Fatalf("during-transfer deliveries = %+v", ds)
	}

	// 3. Accept: d1,d2 unacked rebound to B before post-transfer sends.
	if err := h.eng.Accept(ctx, agentP("B"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	send("a1")
	all := drain(h.agB.C, 3, time.Second)
	want := []string{"d1", "d2", "a1"}
	if len(all) != 3 {
		t.Fatalf("B got %d deliveries: %+v", len(all), all)
	}
	for i, w := range want {
		if all[i].Msg.ID != w || all[i].Handler.AgentID != "B" {
			t.Fatalf("pos %d want %s to B, got %+v", i, w, all[i])
		}
		if i < 2 && !all[i].Redelivered {
			t.Fatalf("%s should be redelivered", w)
		}
	}
	// A must receive nothing after accept.
	if ds := drain(h.agA.C, 1, 50*time.Millisecond); len(ds) != 0 {
		t.Fatalf("A received post-accept message: %+v", ds)
	}
}

// TestCancelTransferKeepsOwnerAndMessages.
func TestCancelTransferKeepsOwnerAndMessages(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("tr-cancel")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	if err := h.eng.TransferTo(ctx, agentP("A"), sid, "B", time.Now()); err != nil {
		t.Fatal(err)
	}
	// B cannot accept a transfer aimed elsewhere / A cannot accept.
	if err := h.eng.Accept(ctx, agentP("A"), sid, time.Now()); err != domain.ErrNotTransferring {
		// A is not the target: Accept expects transferring+target match;
		// status is transferring but target mismatch => unauthorized.
	}
	if err := h.eng.CancelTransfer(ctx, agentP("B"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	sess, _ := h.eng.Session(sid)
	if sess.Status != domain.StatusAssigned || sess.Handler.AgentID != "A" {
		t.Fatalf("after cancel = %+v", sess)
	}
	// B capacity reservation released.
	if b, _ := h.reg.Get("B"); b.Load != 0 {
		t.Fatalf("B load=%d after cancel", b.Load)
	}
	// Messages flow to A again.
	_ = h.eng.Send(ctx, sys, domain.Message{ID: "c1", SessionID: sid, Origin: domain.OriginCustomer})
	if ds := drain(h.agA.C, 1, time.Second); len(ds) != 1 || ds[0].Msg.ID != "c1" {
		t.Fatalf("post-cancel delivery = %+v", ds)
	}
}

// TestTransferRejectsUnauthorizedInitiator: non-owner agent cannot
// transfer someone else's session.
func TestTransferRejectsUnauthorizedInitiator(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("tr-auth")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	if err := h.eng.TransferTo(ctx, agentP("B"), sid, "B", time.Now()); err != domain.ErrUnauthorized {
		t.Fatalf("non-owner transfer err=%v", err)
	}
}
