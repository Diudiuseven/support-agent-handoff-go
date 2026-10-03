package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"
	"github.com/Diudiuseven/support-agent-handoff-go/store"
)

// TestEndToEnd_RestartWithFileStore drives a realistic flow, throws the
// engine away, and resumes from the on-disk log in a brand-new engine.
func TestEndToEnd_RestartWithFileStore(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "events")
	es, err := store.NewFileStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg := agent.NewRegistry()
	_ = reg.Register(agent.Profile{ID: "A", Queues: []domain.QueueName{"support"}, Capacity: 5})
	_ = reg.Register(agent.Profile{ID: "B", Queues: []domain.QueueName{"support"}, Capacity: 5})
	reg.SetOnline("A", true)
	reg.SetOnline("B", true)

	memAud := audit.NewMemorySink()
	bot := NewChannelInbox(32)
	aIB := NewChannelInbox(32)
	bIB := NewChannelInbox(32)
	inboxes := map[string]Inbox{
		InboxBot:               bot,
		InboxAgentPrefix + "A": aIB,
		InboxAgentPrefix + "B": bIB,
	}
	opts := Options{QueueMaxWait: 0, SweepInterval: 20 * time.Millisecond}
	eng := New(es, reg, queue.NewFIFO(), auth.NewRBACPolicy(), audit.NewLogger(memAud), inboxes, opts)
	if err := eng.Run(ctx); err != nil {
		t.Fatal(err)
	}

	sid := domain.SessionID("e2e-1")
	if err := eng.Open(ctx, sid, "support", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, _ = eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	_ = eng.Send(ctx, sys, domain.Message{ID: "e1", SessionID: sid, Origin: domain.OriginCustomer})
	_ = eng.Send(ctx, sys, domain.Message{ID: "e2", SessionID: sid, Origin: domain.OriginCustomer})
	if ds := drain(aIB.C, 2, time.Second); len(ds) != 2 {
		t.Fatalf("A deliveries = %d", len(ds))
	}
	// Crash without ack.
	if err := eng.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := es.Close(); err != nil {
		t.Fatal(err)
	}

	// ---- Restart process ----
	es2, err := store.NewFileStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg2 := agent.NewRegistry()
	_ = reg2.Register(agent.Profile{ID: "A", Queues: []domain.QueueName{"support"}, Capacity: 5})
	_ = reg2.Register(agent.Profile{ID: "B", Queues: []domain.QueueName{"support"}, Capacity: 5})
	reg2.SetOnline("A", true)
	reg2.SetOnline("B", true)
	eng2 := New(es2, reg2, queue.NewFIFO(), auth.NewRBACPolicy(), audit.NewLogger(memAud), inboxes, opts)
	if err := eng2.Recover(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}

	sess, _ := eng2.Session(sid)
	if sess.Status != domain.StatusAssigned || sess.Handler.AgentID != "A" {
		t.Fatalf("after restart session = %+v", sess)
	}
	got := drain(aIB.C, 2, time.Second)
	if len(got) != 2 || got[0].Msg.ID != "e1" || got[1].Msg.ID != "e2" {
		t.Fatalf("post-restart redelivery = %+v", got)
	}
	for _, d := range got {
		if !d.Redelivered {
			t.Fatal("post-restart delivery must be marked redelivered")
		}
		_ = eng2.Ack(ctx, sid, d.Msg.ID)
	}
	// Continue lifecycle: transfer to B.
	if err := eng2.TransferTo(ctx, agentP("A"), sid, "B", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := eng2.Accept(ctx, agentP("B"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = eng2.Send(ctx, sys, domain.Message{ID: "e3", SessionID: sid, Origin: domain.OriginCustomer})
	ds := drain(bIB.C, 1, time.Second)
	if len(ds) != 1 || ds[0].Msg.ID != "e3" || ds[0].Handler.AgentID != "B" {
		t.Fatalf("B delivery after restart = %+v", ds)
	}
}
