package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/auth"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// TestFullAuditTrail verifies every meaningful step of a handoff
// lifecycle is recorded with actor and reason.
func TestFullAuditTrail(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 0)
	h.registerAgents(t, "A", "B")
	sid := domain.SessionID("audit-1")
	_ = h.eng.Open(ctx, sid, "support", time.Now())
	_, _ = h.eng.Escalate(ctx, sys, sid, domain.ReasonCustomerRequested, time.Now())
	_ = h.eng.TransferTo(ctx, agentP("A"), sid, "B", time.Now())
	_ = h.eng.Accept(ctx, agentP("B"), sid, time.Now())
	_ = h.eng.Close(ctx, admin, sid, domain.ReasonAgentEnded, time.Now())
	// Denied attempt.
	_ = h.eng.Take(ctx, agentP("A"), sid, time.Now())

	recs, _ := h.memAud.Scan(ctx, string(sid))
	wantActions := map[string]bool{
		"session.escalate": false,
		"session.transfer": false,
		"session.accept":   false,
		"session.close":    false,
	}
	var sawDenied bool
	for _, r := range recs {
		if r.Outcome == audit.OutcomeDenied {
			sawDenied = true
		}
		if _, ok := wantActions[r.Action]; ok && r.Outcome == audit.OutcomeAllowed {
			wantActions[r.Action] = true
		}
		if r.Actor == "" {
			t.Fatalf("record without actor: %+v", r)
		}
	}
	for act, seen := range wantActions {
		if !seen {
			t.Fatalf("missing allowed audit record for %s (have %+v)", act, recs)
		}
	}
	if !sawDenied {
		t.Fatal("take on closed session should produce a classified error path; " +
			"also verify denial auditing on unauthorized role")
	}
	// Explicit unauthorized role is denied.
	_ = h.eng.Close(ctx, auth.Principal{ID: "x", Role: auth.Role("hacker")}, domain.SessionID("other"), domain.ReasonCustomerEnded, time.Now())
	recsAll, _ := h.memAud.Scan(ctx, "")
	denied := 0
	for _, r := range recsAll {
		if r.Outcome == audit.OutcomeDenied {
			denied++
		}
	}
	if denied == 0 {
		t.Fatal("no denied audit records at all")
	}
}
