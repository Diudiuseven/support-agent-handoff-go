// Package agent keeps the registry of human agents: online state,
// capacity and concurrent-session load.
package agent

import (
	"sync"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

// Profile describes an agent's static configuration.
type Profile struct {
	ID       domain.AgentID
	Name     string
	Queues   []domain.QueueName // queues the agent serves
	Capacity int                // max concurrent sessions; <=0 means 1
}

// Snapshot is an immutable view of an agent at one instant.
type Snapshot struct {
	Profile Profile
	Online  bool
	Load    int
}

// Available reports whether the agent may take one more session.
func (s Snapshot) Available() bool {
	cap := s.Profile.Capacity
	if cap <= 0 {
		cap = 1
	}
	return s.Online && s.Load < cap
}

type state struct {
	profile  Profile
	online   bool
	reserved map[domain.SessionID]struct{}
}

// Registry tracks agents and their live load.
type Registry interface {
	// Register adds or updates an agent profile.
	Register(p Profile) error
	// SetOnline flips availability status.
	SetOnline(id domain.AgentID, online bool)
	// Get returns the snapshot of one agent.
	Get(id domain.AgentID) (Snapshot, bool)
	// List returns snapshots of all registered agents.
	List() []Snapshot
	// TryReserve atomically marks that agentID will serve one more
	// session. Fails (false) if the agent is offline, unknown or at
	// capacity. It is idempotent per (agent, session).
	TryReserve(agentID domain.AgentID, sessionID domain.SessionID) (bool, error)
	// Release decrements the agent's load for the given session.
	Release(agentID domain.AgentID, sessionID domain.SessionID)
	// LoadFor reports whether the session is currently reserved on agentID.
	LoadFor(agentID domain.AgentID, sessionID domain.SessionID) bool
}

// DefaultRegistry is an in-memory, mutex-protected Registry.
type DefaultRegistry struct {
	mu     sync.RWMutex
	agents map[domain.AgentID]*state
}

// NewRegistry constructs an empty in-memory registry.
func NewRegistry() *DefaultRegistry {
	return &DefaultRegistry{agents: make(map[domain.AgentID]*state)}
}

// Register adds or updates an agent profile. Existing load is preserved.
func (r *DefaultRegistry) Register(p Profile) error {
	if p.ID == "" {
		return domain.ErrInvalidArgument
	}
	if p.Capacity <= 0 {
		p.Capacity = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.agents[p.ID]
	if !ok {
		r.agents[p.ID] = &state{profile: p, reserved: make(map[domain.SessionID]struct{})}
		return nil
	}
	st.profile = p
	return nil
}

// SetOnline flips availability status.
func (r *DefaultRegistry) SetOnline(id domain.AgentID, online bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.agents[id]; ok {
		st.online = online
	}
}

// Get returns the snapshot of one agent.
func (r *DefaultRegistry) Get(id domain.AgentID) (Snapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.agents[id]
	if !ok {
		return Snapshot{}, false
	}
	return Snapshot{Profile: st.profile, Online: st.online, Load: len(st.reserved)}, true
}

// List returns snapshots of all registered agents in registration order
// is not guaranteed; callers needing deterministic ordering sort
// themselves (the engine does).
func (r *DefaultRegistry) List() []Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Snapshot, 0, len(r.agents))
	for _, st := range r.agents {
		out = append(out, Snapshot{Profile: st.profile, Online: st.online, Load: len(st.reserved)})
	}
	return out
}

// TryReserve atomically reserves one capacity slot for sessionID.
//
// It is idempotent per (agent, session): reserving a session the agent
// already holds succeeds without incrementing load, which makes the
// reservation safe to re-run after a crash replay.
func (r *DefaultRegistry) TryReserve(agentID domain.AgentID, sessionID domain.SessionID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.agents[agentID]
	if !ok {
		return false, domain.ErrAgentOffline
	}
	if _, dup := st.reserved[sessionID]; dup {
		return true, nil // idempotent re-reservation
	}
	cap := st.profile.Capacity
	if cap <= 0 {
		cap = 1
	}
	if !st.online {
		return false, domain.ErrAgentOffline
	}
	if len(st.reserved) >= cap {
		return false, domain.ErrAgentAtCapacity
	}
	st.reserved[sessionID] = struct{}{}
	return true, nil
}

// Release decrements the agent's load for the given session. It is a
// no-op when the session was not reserved, so it is safe to call during
// recovery and on duplicate paths.
func (r *DefaultRegistry) Release(agentID domain.AgentID, sessionID domain.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.agents[agentID]; ok {
		delete(st.reserved, sessionID)
	}
}

// LoadFor reports whether the session is currently reserved on agentID.
func (r *DefaultRegistry) LoadFor(agentID domain.AgentID, sessionID domain.SessionID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.agents[agentID]
	if !ok {
		return false
	}
	_, held := st.reserved[sessionID]
	return held
}
