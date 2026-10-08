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

// cloneState 深拷贝全部状态（用于事务回滚/隔离）
func cloneState(sessions map[model.SessionID]*model.Session,
	agents map[model.AgentID]*model.Agent,
	messages map[model.SessionID][]*model.Message,
	handoffs map[string]*model.Handoff,
	audits map[model.SessionID][]*model.AuditEvent,
	queue map[model.SessionID]*model.QueueEntry,
) (map[model.SessionID]*model.Session,
	map[model.AgentID]*model.Agent,
	map[model.SessionID][]*model.Message,
	map[string]*model.Handoff,
	map[model.SessionID][]*model.AuditEvent,
	map[model.SessionID]*model.QueueEntry,
) {
	cs := make(map[model.SessionID]*model.Session, len(sessions))
	for k, v := range sessions {
		cs[k] = cloneSession(v)
	}
	ca := make(map[model.AgentID]*model.Agent, len(agents))
	for k, v := range agents {
		ca[k] = cloneAgent(v)
	}
	cm := make(map[model.SessionID][]*model.Message, len(messages))
	for k, v := range messages {
		cp := make([]*model.Message, len(v))
		for i, mm := range v {
			c := *mm
			cp[i] = &c
		}
		cm[k] = cp
	}
	ch := make(map[string]*model.Handoff, len(handoffs))
	for k, v := range handoffs {
		c := *v
		ch[k] = &c
	}
	cau := make(map[model.SessionID][]*model.AuditEvent, len(audits))
	for k, v := range audits {
		cp := make([]*model.AuditEvent, len(v))
		for i, e := range v {
			c := *e
			cp[i] = &c
		}
		cau[k] = cp
	}
	cq := make(map[model.SessionID]*model.QueueEntry, len(queue))
	for k, v := range queue {
		c := *v
		cq[k] = &c
	}
	return cs, ca, cm, ch, cau, cq
}

func (m *MemStore) snapshotState() (map[model.SessionID]*model.Session,
	map[model.AgentID]*model.Agent,
	map[model.SessionID][]*model.Message,
	map[string]*model.Handoff,
	map[model.SessionID][]*model.AuditEvent,
	map[model.SessionID]*model.QueueEntry,
) {
	return cloneState(m.sessions, m.agents, m.messages, m.handoffs, m.audits, m.queue)
}

// Update 在一个事务内批量修改状态：
//   - fn 在一份隔离的状态副本（事务 Store）上执行，期间的多次写操作互不交错；
//   - fn 返回 nil 时事务结果整体替换当前状态；返回错误时整体回滚，状态不变。
//
// 因此会话、交接单、审计、坐席计数等多条记录的联动变更要么整体生效，
// 要么整体不生效，不会出现半完成的中间态。
func (m *MemStore) Update(ctx context.Context, fn func(tx Store) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions, agents, messages, handoffs, audits, queue := m.snapshotState()
	tx := &MemStore{
		sessions: sessions,
		agents:   agents,
		messages: messages,
		handoffs: handoffs,
		audits:   audits,
		queue:    queue,
	}
	if err := fn(tx); err != nil {
		return err
	}
	m.sessions = tx.sessions
	m.agents = tx.agents
	m.messages = tx.messages
	m.handoffs = tx.handoffs
	m.audits = tx.audits
	m.queue = tx.queue
	return nil
}
