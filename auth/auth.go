// Package auth implements the permission policy for handoff and
// distribution operations.
//
// The policy is deliberately coarse-grained (role + identity match on
// the target agent). Resource-level rules that depend on live session
// state — for example "an agent may only transfer a session they
// currently own" — are enforced by the engine in addition to this
// policy, and every denial is written to the audit trail.
package auth

import "github.com/Diudiuseven/support-agent-handoff-go/domain"

// Role of an authenticated principal.
type Role string

const (
	RoleSystem Role = "system" // the bot / scheduler
	RoleAgent  Role = "agent"  // a human agent, identity must match
	RoleAdmin  Role = "admin"  // supervisor, may act on any session
)

// Principal is the authenticated caller of an engine operation.
type Principal struct {
	ID   string
	Role Role
}

// Action identifiers checked by the policy.
type Action string

const (
	ActionEscalate Action = "session.escalate"
	ActionTake     Action = "session.take"
	ActionAccept   Action = "session.accept"
	ActionRelease  Action = "session.release"
	ActionTransfer Action = "session.transfer"
	ActionClose    Action = "session.close"
	ActionSend     Action = "session.send"
)

// Policy authorizes operations on sessions.
type Policy interface {
	// Can reports whether p may perform action. targetAgent is the agent
	// that would own / is accepting the session (empty for bot actions).
	Can(p Principal, action Action, targetAgent domain.AgentID) bool
}

// RBACPolicy is the default role-based policy.
//
// Rules:
//
//   - system: escalate, release, send, close (scheduler/bot actions).
//   - admin:  every action, on any session/agent.
//   - agent:  take/accept only for itself (p.ID == targetAgent);
//     transfer any session (ownership is checked by engine);
//     close/send any session (ownership checked by engine).
type RBACPolicy struct{}

// NewRBACPolicy constructs the default policy.
func NewRBACPolicy() *RBACPolicy { return &RBACPolicy{} }

// Can implements Policy.
func (*RBACPolicy) Can(p Principal, action Action, targetAgent domain.AgentID) bool {
	switch p.Role {
	case RoleAdmin:
		return true
	case RoleSystem:
		switch action {
		case ActionEscalate, ActionRelease, ActionSend, ActionClose, ActionTake:
			return true
		}
		return false
	case RoleAgent:
		switch action {
		case ActionTake, ActionAccept:
			// An agent may only grab/accept for itself.
			return targetAgent != "" && p.ID == string(targetAgent)
		case ActionTransfer, ActionClose, ActionSend:
			return true
		}
		return false
	}
	return false
}
