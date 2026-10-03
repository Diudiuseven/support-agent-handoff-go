package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestNonOwnerCannotCloseOrSpeakAsAgent: resource-level ownership
// checks beyond the coarse RBAC role check.
func TestNonOwnerCannotCloseOrSpeakAsAgent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("authz-1")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	// B is an agent but not the owner: cannot close.
	if err := h.eng.Close(ctx, agentP("B"), sid, domain.ReasonAgentEnded, time.Now()); err != domain.ErrUnauthorized {
		t.Fatalf("non-owner close err=%v", err)
	}
	// B cannot send an agent-origin reply attributed to A's session.
	err := h.eng.Send(ctx, agentP("B"), domain.Message{ID: "x", SessionID: sid, Origin: domain.OriginAgent})
	if err != domain.ErrUnauthorized {
		t.Fatalf("non-owner agent send err=%v", err)
	}
	// System escalation by an agent role is forbidden at RBAC layer.
	if _, err := h.eng.Escalate(ctx, agentP("A"), sid, domain.ReasonCustomerRequested, time.Now()); err != domain.ErrUnauthorized {
		t.Fatalf("agent escalate err=%v", err)
	}
	// Owner A can do both.
	if err := h.eng.Send(ctx, agentP("A"), domain.Message{ID: "reply-1", SessionID: sid, Origin: domain.OriginAgent, Text: "hello"}); err != nil {
		t.Fatalf("owner reply: %v", err)
	}
	if err := h.eng.Close(ctx, agentP("A"), sid, domain.ReasonAgentEnded, time.Now()); err != nil {
		t.Fatalf("owner close: %v", err)
	}
	// All denials audited.
	recs, _ := h.memAud.Scan(ctx, string(sid))
	denied := 0
	for _, r := range recs {
		if r.Outcome == audit.OutcomeDenied {
			denied++
			if r.Actor == "" || r.Reason == "" {
				t.Fatalf("denied record missing actor/reason: %+v", r)
			}
		}
	}
	if denied < 3 {
		t.Fatalf("want >=3 denied records, got %d: %+v", denied, recs)
	}
}

// TestTakeLoserIsAuditedAndSessionStable: competing takes produce one
// allowed + n-1 denied records, and the loser cannot later act as owner.
func TestTakeLoserIsAuditedAndSessionStable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	// Agents offline at escalate time so the session queues.
	_ = h.reg.Register(regProfile("A", 1))
	_ = h.reg.Register(regProfile("B", 1))
	sid := domain.SessionID("authz-2")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())

	// A takes it online.
	h.reg.SetOnline("A", true)
	if err := h.eng.Take(ctx, agentP("A"), sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	// B comes online and loses.
	h.reg.SetOnline("B", true)
	if err := h.eng.Take(ctx, agentP("B"), sid, time.Now()); err != domain.ErrAlreadyAssigned {
		t.Fatalf("loser err=%v", err)
	}
	recs, _ := h.memAud.Scan(ctx, string(sid))
	var allowedTake, denied int
	for _, r := range recs {
		if r.Action == "session.take" && r.Outcome == audit.OutcomeAllowed {
			allowedTake++
		}
		if r.Outcome == audit.OutcomeDenied {
			denied++
		}
	}
	if allowedTake != 1 || denied < 1 {
		t.Fatalf("take allowed=%d denied=%d recs=%+v", allowedTake, denied, recs)
	}
	// Loser B still cannot reply as the owning agent.
	if err := h.eng.Send(ctx, agentP("B"), domain.Message{ID: "z", SessionID: sid, Origin: domain.OriginAgent}); err != domain.ErrUnauthorized {
		t.Fatalf("loser reply err=%v", err)
	}
}
