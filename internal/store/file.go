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
type FileStore struct {
	*MemStore
	dir string
	mu  sync.Mutex
}

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
	f.MemStore.mu.RLock()
	defer f.MemStore.mu.RUnlock()
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
