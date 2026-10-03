// Command handoff-demo walks one customer session through the complete
// lifecycle: bot handling -> escalation -> queue -> agent pickup ->
// transfer to another agent -> fallback -> close, printing each step.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/engine"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"
	"github.com/Diudiuseven/support-agent-handoff-go/store"
)

func main() {
	ctx := context.Background()

	es := store.NewMemoryStore()
	reg := agent.NewRegistry()
	_ = reg.Register(agent.Profile{ID: "alice", Name: "Alice", Queues: []domain.QueueName{"billing"}, Capacity: 2})
	_ = reg.Register(agent.Profile{ID: "bob", Name: "Bob", Queues: []domain.QueueName{"billing"}, Capacity: 2})
	reg.SetOnline("alice", true)

	memAudit := audit.NewMemorySink()
	inboxes, bot, agInboxes := engine.NewDefaultInboxes(16, "alice", "bob")

	eng := engine.New(es, reg, queue.NewFIFO(), auth.NewRBACPolicy(),
		audit.NewLogger(memAudit), inboxes,
		engine.Options{QueueMaxWait: 30 * time.Second, SweepInterval: 100 * time.Millisecond})
	if err := eng.Run(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop()

	sys := auth.Principal{ID: "bot", Role: auth.RoleSystem}
	alice := auth.Principal{ID: "alice", Role: auth.RoleAgent}
	bob := auth.Principal{ID: "bob", Role: auth.RoleAgent}

	sid := domain.SessionID("cust-1001")
	must(eng.Open(ctx, sid, "billing", time.Now()))
	fmt.Println("1) session opened, owner = bot")

	must(eng.Send(ctx, sys, domain.Message{ID: "m1", SessionID: sid, Origin: domain.OriginCustomer, Text: "My invoice looks wrong"}))
	recv(bot.C, "bot")
	eng.Ack(ctx, sid, "m1")

	// Alice is online and free -> immediate assignment.
	res := must2(eng.Escalate(ctx, sys, sid, domain.ReasonBotEscalated, time.Now()))
	fmt.Printf("2) escalated -> %s, agent=%s, reason=%s\n", res.Status, res.AssignedAgent, res.Reason)

	must(eng.Send(ctx, sys, domain.Message{ID: "m2", SessionID: sid, Origin: domain.OriginCustomer, Text: "Can a human look at this?"}))
	recv(agInboxes["alice"].C, "alice")

	// Transfer to bob (who just came online mid-conversation).
	reg.SetOnline("bob", true)
	must(eng.TransferTo(ctx, alice, sid, "bob", time.Now()))
	fmt.Println("3) transfer alice -> bob started (messages still go to alice until accept)")
	must(eng.Accept(ctx, bob, sid, time.Now()))
	fmt.Println("4) bob accepted; pending messages rebound to bob in order")
	recv(agInboxes["bob"].C, "bob") // redelivered m2
	eng.Ack(ctx, sid, "m2")

	must(eng.Send(ctx, sys, domain.Message{ID: "m3", SessionID: sid, Origin: domain.OriginCustomer, Text: "Thanks!"}))
	recv(agInboxes["bob"].C, "bob")
	eng.Ack(ctx, sid, "m3")

	must(eng.Close(ctx, bob, sid, domain.ReasonCustomerEnded, time.Now()))
	fmt.Println("5) session closed; capacity released")

	recs, _ := memAudit.Scan(ctx, string(sid))
	fmt.Printf("6) audit trail for %s (%d records):\n", sid, len(recs))
	for _, r := range recs {
		tgt := r.Target
		if tgt == "" {
			tgt = "-"
		}
		fmt.Printf("   %s  %-24s actor=%-6s target=%-6s reason=%s\n",
			r.Time.Format("15:04:05"), r.Action, r.Actor, tgt, r.Reason)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func must2[T any](v T, err error) T {
	must(err)
	return v
}

func recv(ch <-chan engine.Delivery, who string) {
	select {
	case d := <-ch:
		redel := ""
		if d.Redelivered {
			redel = " [redelivered]"
		}
		fmt.Printf("   -> %s received %q from %s%s\n", who, d.Msg.Text, d.Msg.Origin, redel)
	case <-time.After(time.Second):
		fmt.Printf("   !! %s received nothing\n", who)
	}
}
