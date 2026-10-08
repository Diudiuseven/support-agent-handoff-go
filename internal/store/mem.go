package store

import (
	"context"
	"sync"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// MemStore 内存实现（FileStore 内嵌它并在每次写后原子快照到磁盘）
type MemStore struct {
	mu       sync.RWMutex
	sessions map[model.SessionID]*model.Session
	agents   map[model.AgentID]*model.Agent
	messages map[model.SessionID][]*model.Message
	handoffs map[string]*model.Handoff
	audits   map[model.SessionID][]*model.AuditEvent
	queue    map[model.SessionID]*model.QueueEntry
}

func NewMemStore() *MemStore {
	return &MemStore{
		sessions: make(map[model.SessionID]*model.Session),
		agents:   make(map[model.AgentID]*model.Agent),
		messages: make(map[model.SessionID][]*model.Message),
		handoffs: make(map[string]*model.Handoff),
		audits:   make(map[model.SessionID][]*model.AuditEvent),
		queue:    make(map[model.SessionID]*model.QueueEntry),
	}
}

func (m *MemStore) Init(ctx context.Context) error { return nil }
func (m *MemStore) Close() error                   { return nil }

// BeginTx 在当前状态的深拷贝上开启事务；Commit 时工作副本整体替换原状态。
func (m *MemStore) BeginTx(ctx context.Context) (Tx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w := NewMemStore()
	w.sessions = cloneMap(m.sessions, cloneSession)
	w.agents = cloneMap(m.agents, cloneAgent)
	w.messages = cloneMessages(m.messages)
	w.handoffs = cloneMap(m.handoffs, func(h *model.Handoff) *model.Handoff { cp := *h; return &cp })
	w.audits = cloneAudits(m.audits)
	w.queue = cloneMap(m.queue, func(q *model.QueueEntry) *model.QueueEntry { cp := *q; return &cp })
	return &memTx{MemStore: w, parent: m}, nil
}

// memTx 在独立的 MemStore 工作副本上读写；Commit 时原子替换父 store 的全部 map。
type memTx struct {
	*MemStore
	parent   *MemStore
	finished bool
}

func (t *memTx) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	t.parent.mu.Lock()
	defer t.parent.mu.Unlock()
	t.MemStore.mu.Lock()
	defer t.MemStore.mu.Unlock()
	t.parent.sessions = t.MemStore.sessions
	t.parent.agents = t.MemStore.agents
	t.parent.messages = t.MemStore.messages
	t.parent.handoffs = t.MemStore.handoffs
	t.parent.audits = t.MemStore.audits
	t.parent.queue = t.MemStore.queue
	return nil
}

func (t *memTx) Rollback() {
	t.finished = true // 丢弃工作副本即可
}

func cloneMap[K comparable, V any](src map[K]V, cp func(V) V) map[K]V {
	dst := make(map[K]V, len(src))
	for k, v := range src {
		dst[k] = cp(v)
	}
	return dst
}

func cloneMessages(src map[model.SessionID][]*model.Message) map[model.SessionID][]*model.Message {
	dst := make(map[model.SessionID][]*model.Message, len(src))
	for k, list := range src {
		cp := make([]*model.Message, len(list))
		for i, m := range list {
			cm := *m
			cp[i] = &cm
		}
		dst[k] = cp
	}
	return dst
}

func cloneAudits(src map[model.SessionID][]*model.AuditEvent) map[model.SessionID][]*model.AuditEvent {
	dst := make(map[model.SessionID][]*model.AuditEvent, len(src))
	for k, list := range src {
		cp := make([]*model.AuditEvent, len(list))
		for i, e := range list {
			ce := *e
			cp[i] = &ce
		}
		dst[k] = cp
	}
	return dst
}

func cloneSession(s *model.Session) *model.Session {
	cp := *s
	return &cp
}

func cloneAgent(a *model.Agent) *model.Agent {
	cp := *a
	cp.Skills = append([]string(nil), a.Skills...)
	return &cp
}

func (m *MemStore) PutSession(ctx context.Context, s *model.Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = cloneSession(s)
	return nil
}

func (m *MemStore) GetSession(ctx context.Context, id model.SessionID) (*model.Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneSession(s), nil
}

func (m *MemStore) ListSessions(ctx context.Context) ([]*model.Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, cloneSession(s))
	}
	return out, nil
}

func (m *MemStore) PutAgent(ctx context.Context, a *model.Agent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.agents[a.ID] = cloneAgent(a)
	return nil
}

func (m *MemStore) GetAgent(ctx context.Context, id model.AgentID) (*model.Agent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.agents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneAgent(a), nil
}

func (m *MemStore) ListAgents(ctx context.Context) ([]*model.Agent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.Agent, 0, len(m.agents))
	for _, a := range m.agents {
		out = append(out, cloneAgent(a))
	}
	return out, nil
}

func (m *MemStore) AppendMessage(ctx context.Context, msg *model.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages[msg.SessionID] = append(m.messages[msg.SessionID], msg)
	return nil
}

func (m *MemStore) UpdateMessage(ctx context.Context, msg *model.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.messages[msg.SessionID] {
		if existing.ID == msg.ID {
			cp := *msg
			m.messages[msg.SessionID][i] = &cp
			return nil
		}
	}
	return ErrNotFound
}

func (m *MemStore) ListMessages(ctx context.Context, sessionID model.SessionID) ([]*model.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.Message, len(m.messages[sessionID]))
	copy(out, m.messages[sessionID])
	return out, nil
}

func (m *MemStore) PutHandoff(ctx context.Context, h *model.Handoff) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *h
	m.handoffs[h.ID] = &cp
	return nil
}

func (m *MemStore) GetHandoff(ctx context.Context, id string) (*model.Handoff, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.handoffs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *h
	return &cp, nil
}

func (m *MemStore) ListHandoffs(ctx context.Context, sessionID model.SessionID) ([]*model.Handoff, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*model.Handoff
	for _, h := range m.handoffs {
		if h.SessionID == sessionID {
			cp := *h
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (m *MemStore) AppendAudit(ctx context.Context, e *model.AuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audits[e.SessionID] = append(m.audits[e.SessionID], e)
	return nil
}

func (m *MemStore) ListAudit(ctx context.Context, sessionID model.SessionID) ([]*model.AuditEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.AuditEvent, len(m.audits[sessionID]))
	copy(out, m.audits[sessionID])
	return out, nil
}

func (m *MemStore) PutQueueEntry(ctx context.Context, q *model.QueueEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *q
	m.queue[q.SessionID] = &cp
	return nil
}

func (m *MemStore) DeleteQueueEntry(ctx context.Context, sessionID model.SessionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.queue, sessionID)
	return nil
}

func (m *MemStore) ListQueue(ctx context.Context) ([]*model.QueueEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.QueueEntry, 0, len(m.queue))
	for _, q := range m.queue {
		cp := *q
		out = append(out, &cp)
	}
	return out, nil
}
