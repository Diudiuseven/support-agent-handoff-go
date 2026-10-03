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
	"github.com/Diudiuseven/support-agent-handoff-go/store"
)

type harness struct {
	eng    *Engine
	reg    *agent.DefaultRegistry
	memAud *audit.MemorySink
	bot    *ChannelInbox
	agA    *ChannelInbox
	agB    *ChannelInbox
}

func newHarness(t *testing.T, maxWait time.Duration) *harness {
	t.Helper()
	es := store.NewMemoryStore()
	reg := agent.NewRegistry()
	q := queue.NewFIFO()
	pol := auth.NewRBACPolicy()
	memAud := audit.NewMemorySink()
	aud := audit.NewLogger(memAud)
	bot := NewChannelInbox(16)
	agA := NewChannelInbox(16)
	agB := NewChannelInbox(16)
	eng := New(es, reg, q, pol, aud, map[string]Inbox{
		InboxBot:               bot,
		InboxAgentPrefix + "A": agA,
		InboxAgentPrefix + "B": agB,
	}, Options{QueueMaxWait: maxWait, SweepInterval: 50 * time.Millisecond})
	return &harness{eng: eng, reg: reg, memAud: memAud, bot: bot, agA: agA, agB: agB}
}

func (h *harness) registerAgents(t *testing.T, online ...string) {
	t.Helper()
	for _, id := range online {
		if err := h.reg.Register(agent.Profile{
			ID: domain.AgentID(id), Name: id,
			Queues: []domain.QueueName{"support"}, Capacity: 2,
		}); err != nil {
			t.Fatal(err)
		}
		h.reg.SetOnline(domain.AgentID(id), true)
	}
}

var (
	sys   = auth.Principal{ID: "sys", Role: auth.RoleSystem}
	admin = auth.Principal{ID: "boss", Role: auth.RoleAdmin}
)

func agentP(id string) auth.Principal { return auth.Principal{ID: id, Role: auth.RoleAgent} }

func drain(ch chan Delivery, n int, timeout time.Duration) []Delivery {
	out := make([]Delivery, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case d := <-ch:
			out = append(out, d)
		case <-deadline:
			return out
		}
	}
	return out
}

func TestHappyPath_EscalateTakeTransferClose(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")

	sid := domain.SessionID("s1")
	if err := h.eng.Open(ctx, sid, "support", time.Now()); err != nil {
		t.Fatal(err)
	}

	// Customer message while bot owns it -> bot inbox.
	if err := h.eng.Send(ctx, sys, domain.Message{ID: "m1", SessionID: sid, Origin: domain.OriginCustomer, Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	got := drain(h.bot.C, 1, time.Second)
	if len(got) != 1 || got[0].Handler.Kind != domain.HandlerBot {
		t.Fatalf("bot delivery = %+v", got)
	}
	if err := h.eng.Ack(ctx, sid, "m1"); err != nil {
		t.Fatal(err)
	}

	// Escalate with A online and free -> immediate assignment.
	res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusAssigned || res.AssignedAgent != "A" {
		t.Fatalf("escalate = %+v", res)
	}

	// Message during agent ownership -> A inbox.
	if err := h.eng.Send(ctx, sys, domain.Message{ID: "m2", SessionID: sid, Origin: domain.OriginCustomer, Text: "help"}); err != nil {
		t.Fatal(err)
	}
	gotA := drain(h.agA.C, 1, time.Second)
	if len(gotA) != 1 || gotA[0].Handler.AgentID != "A" {
		t.Fatalf("agent A delivery = %+v", gotA)
	}

	// Transfer A -> B: messages mid-transfer stay with A.
	if err := h.eng.TransferTo(ctx, agentP("A"), sid, "B", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Send(ctx, sys, domain.Message{ID: "m3", SessionID: sid, Origin: domain.OriginCustomer, Text: "during"}); err != nil {
		t.Fatal(err)
	}
	if ds := drain(h.agA.C, 1, 100*time.Millisecond); len(ds) != 1 || ds[0].Msg.ID != "m3" {
		t.Fatalf("mid-transfer message must stay with A, got %+v", ds)
	}

	if err := h.eng.Accept(ctx, agentP("B"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	sess, ok := h.eng.Session(sid)
	if !ok || sess.Status != domain.StatusAssigned || sess.Handler.AgentID != "B" {
		t.Fatalf("session after accept = %+v ok=%v", sess, ok)
	}
	// m2,m3 unacked must rebound and be redelivered to B in order.
	reb := drain(h.agB.C, 2, time.Second)
	if len(reb) != 2 || reb[0].Msg.ID != "m2" || reb[1].Msg.ID != "m3" {
		t.Fatalf("rebound order = %+v", reb)
	}
	for _, d := range reb {
		if !d.Redelivered {
			t.Fatalf("expected redelivery flag on %s", d.Msg.ID)
		}
		if err := h.eng.Ack(ctx, sid, d.Msg.ID); err != nil {
			t.Fatal(err)
		}
	}

	if err := h.eng.Close(ctx, agentP("B"), sid, domain.ReasonCustomerEnded, time.Now()); err != nil {
		t.Fatal(err)
	}
	sess, _ = h.eng.Session(sid)
	if sess.Status != domain.StatusClosed {
		t.Fatalf("status = %s", sess.Status)
	}
	if load, _ := h.reg.Get("B"); load.Load != 0 {
		t.Fatalf("B load after close = %d", load.Load)
	}
	if load, _ := h.reg.Get("A"); load.Load != 0 {
		t.Fatalf("A load after close = %d", load.Load)
	}
}

func TestUnauthorizedTakeIsDeniedAndAudited(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	// No agents online: escalation puts the session in the queue.
	if err := h.reg.Register(agent.Profile{
		ID: "A", Queues: []domain.QueueName{"support"}, Capacity: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.Register(agent.Profile{
		ID: "B", Queues: []domain.QueueName{"support"}, Capacity: 1,
	}); err != nil {
		t.Fatal(err)
	}
	h.reg.SetOnline("A", true)

	sid := domain.SessionID("s2")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	res, err := h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.StatusAssigned || res.AssignedAgent != "A" {
		t.Fatalf("A should be assigned, got %+v", res)
	}
	// B cannot grab an already-assigned session (distinct error, auditable).
	if err := h.eng.Take(ctx, agentP("B"), sid, time.Now()); err != domain.ErrAlreadyAssigned {
		t.Fatalf("want ErrAlreadyAssigned, got %v", err)
	}
	// Unknown role principal must be denied at the policy layer.
	err = h.eng.Take(ctx, auth.Principal{ID: "x", Role: auth.Role("hacker")}, sid, time.Now())
	if err != domain.ErrUnauthorized {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
	recs, _ := h.memAud.Scan(ctx, "")
	found := false
	for _, r := range recs {
		if r.Outcome == audit.OutcomeDenied {
			found = true
		}
	}
	if !found {
		t.Fatal("denied attempt was not audited")
	}
}
