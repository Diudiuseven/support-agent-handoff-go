package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

const snapshotFile = "snapshot.json"

// FileStore 在 MemStore 基础上，每次写操作后把全量状态原子快照到磁盘，
// 崩溃/重启后可从快照恢复（见 service.Recover）。
// 写盘采用"临时文件 + fsync + rename + 目录 fsync"，保证快照不损坏。
// 事务（BeginTx）在工作副本上累积变更，Commit 时整体替换内存并只写一次
// 原子快照，因此一个业务操作内的多条记录要么全部生效，要么全部不生效。
type FileStore struct {
	*MemStore
	dir string
	mu  sync.Mutex
}

// BeginTx 返回一个 FileTx：工作副本为 MemStore 深拷贝，期间不触碰父状态，
// Commit 时在同一把 f.mu 内替换内存并原子落盘。
func (f *FileStore) BeginTx(ctx context.Context) (Tx, error) {
	work, err := f.MemStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &fileTx{file: f, memTx: work.(*memTx)}, nil
}

type fileTx struct {
	file *FileStore
	*memTx
	finished bool
}

func (t *fileTx) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	t.file.mu.Lock()
	defer t.file.mu.Unlock()
	if err := t.memTx.Commit(); err != nil {
		return err
	}
	return t.file.persistLocked()
}

func (t *fileTx) Rollback() {
	if t.finished {
		return
	}
	t.finished = true
	t.memTx.Rollback()
}

// Init/Close 委托给嵌入的工作副本（覆盖从 *FileStore 侧误用的可能）
func (t *fileTx) Init(ctx context.Context) error { return t.memTx.Init(ctx) }
func (t *fileTx) Close() error                   { return t.memTx.Close() }

type snapshot struct {
	Sessions map[model.SessionID]*model.Session      `json:"sessions"`
	Agents   map[model.AgentID]*model.Agent          `json:"agents"`
	Messages map[model.SessionID][]*model.Message    `json:"messages"`
	Handoffs map[string]*model.Handoff               `json:"handoffs"`
	Audits   map[model.SessionID][]*model.AuditEvent `json:"audits"`
	Queue    map[model.SessionID]*model.QueueEntry   `json:"queue"`
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	return &FileStore{MemStore: NewMemStore(), dir: dir}, nil
}

func (f *FileStore) Init(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	path := filepath.Join(f.dir, snapshotFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("parse snapshot: %w", err)
	}
	f.MemStore.mu.Lock()
	defer f.MemStore.mu.Unlock()
	if snap.Sessions != nil {
		f.MemStore.sessions = snap.Sessions
	}
	if snap.Agents != nil {
		f.MemStore.agents = snap.Agents
	}
	if snap.Messages != nil {
		f.MemStore.messages = snap.Messages
	}
	if snap.Handoffs != nil {
		f.MemStore.handoffs = snap.Handoffs
	}
	if snap.Audits != nil {
		f.MemStore.audits = snap.Audits
	}
	if snap.Queue != nil {
		f.MemStore.queue = snap.Queue
	}
	return nil
}

func (f *FileStore) persistLocked() error {
	f.MemStore.mu.RLock()
	defer f.MemStore.mu.RUnlock()
	snap := snapshot{}
	snap.Sessions = make(map[model.SessionID]*model.Session, len(f.MemStore.sessions))
	for k, v := range f.MemStore.sessions {
		snap.Sessions[k] = cloneSession(v)
	}
	snap.Agents = make(map[model.AgentID]*model.Agent, len(f.MemStore.agents))
	for k, v := range f.MemStore.agents {
		snap.Agents[k] = cloneAgent(v)
	}
	snap.Messages = make(map[model.SessionID][]*model.Message, len(f.MemStore.messages))
	for k, v := range f.MemStore.messages {
		cp := make([]*model.Message, len(v))
		copy(cp, v)
		snap.Messages[k] = cp
	}
	snap.Handoffs = make(map[string]*model.Handoff, len(f.MemStore.handoffs))
	for k, v := range f.MemStore.handoffs {
		cp := *v
		snap.Handoffs[k] = &cp
	}
	snap.Audits = make(map[model.SessionID][]*model.AuditEvent, len(f.MemStore.audits))
	for k, v := range f.MemStore.audits {
		cp := make([]*model.AuditEvent, len(v))
		copy(cp, v)
		snap.Audits[k] = cp
	}
	snap.Queue = make(map[model.SessionID]*model.QueueEntry, len(f.MemStore.queue))
	for k, v := range f.MemStore.queue {
		cp := *v
		snap.Queue[k] = &cp
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(f.dir, "snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	path := filepath.Join(f.dir, snapshotFile)
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	if d, err := os.Open(f.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (f *FileStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.persistLocked()
}

// --- 以下写操作在 MemStore 基础上追加原子快照 ---

func (f *FileStore) PutSession(ctx context.Context, s *model.Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.PutSession(ctx, s); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) PutAgent(ctx context.Context, a *model.Agent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.PutAgent(ctx, a); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) AppendMessage(ctx context.Context, m *model.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.AppendMessage(ctx, m); err != nil {
		return err
	}
	return f.persistLocked()
}

// UpdateMessage 覆写：MemStore 内嵌版本不会触发快照，若不覆写会导致
// 消息 Delivered 标记只存在于内存，重启后积压消息被重复投递。
func (f *FileStore) UpdateMessage(ctx context.Context, m *model.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.UpdateMessage(ctx, m); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) PutHandoff(ctx context.Context, h *model.Handoff) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.PutHandoff(ctx, h); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) AppendAudit(ctx context.Context, e *model.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.AppendAudit(ctx, e); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) PutQueueEntry(ctx context.Context, q *model.QueueEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.PutQueueEntry(ctx, q); err != nil {
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) DeleteQueueEntry(ctx context.Context, sessionID model.SessionID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.MemStore.DeleteQueueEntry(ctx, sessionID); err != nil {
		return err
	}
	return f.persistLocked()
}

// LoadSnapshotForTest 用给定快照一次性替换全部内存状态并原子落盘。
// 仅用于测试：构造"崩溃后留下的半完成快照"以验证 Recover 的幂等修复。
func (f *FileStore) LoadSnapshotForTest(snap SnapshotData) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.MemStore.mu.Lock()
	f.MemStore.sessions = cloneMap(snap.Sessions, cloneSession)
	f.MemStore.agents = cloneMap(snap.Agents, cloneAgent)
	f.MemStore.messages = cloneMessages(snap.Messages)
	f.MemStore.handoffs = cloneMap(snap.Handoffs, func(h *model.Handoff) *model.Handoff { cp := *h; return &cp })
	f.MemStore.audits = cloneAudits(snap.Audits)
	f.MemStore.queue = cloneMap(snap.Queue, func(q *model.QueueEntry) *model.QueueEntry { cp := *q; return &cp })
	f.MemStore.mu.Unlock()
	return f.persistLocked()
}

// SnapshotData 快照数据（测试注入用）
type SnapshotData struct {
	Sessions map[model.SessionID]*model.Session
	Agents   map[model.AgentID]*model.Agent
	Messages map[model.SessionID][]*model.Message
	Handoffs map[string]*model.Handoff
	Audits   map[model.SessionID][]*model.AuditEvent
	Queue    map[model.SessionID]*model.QueueEntry
}
