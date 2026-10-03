package auth

import (
	"testing"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func TestRBAC(t *testing.T) {
	pol := NewRBACPolicy()
	sys := Principal{ID: "sys", Role: RoleSystem}
	admin := Principal{ID: "boss", Role: RoleAdmin}
	a := Principal{ID: "A", Role: RoleAgent}

	cases := []struct {
		p      Principal
		act    Action
		target domain.AgentID
		want   bool
	}{
		{sys, ActionEscalate, "", true},
		{sys, ActionTransfer, "A", false},
		{admin, ActionTransfer, "A", true},
		{admin, ActionTake, "B", true},
		{a, ActionTake, "A", true},
		{a, ActionTake, "B", false}, // cannot take on behalf of another
		{a, ActionAccept, "A", true},
		{a, ActionAccept, "B", false},
		{a, ActionTransfer, "B", true}, // ownership checked by engine
		{a, ActionClose, "", true},
		{Principal{ID: "x", Role: Role("hacker")}, ActionTake, "x", false},
	}
	for i, c := range cases {
		if got := pol.Can(c.p, c.act, c.target); got != c.want {
			t.Errorf("case %d (%s,%s,%s)=%v want %v", i, c.p.ID, c.act, c.target, got, c.want)
		}
	}
}
