package store

import (
	"context"
	"errors"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

var (
	// ErrNotFound 记录不存在
	ErrNotFound = errors.New("store: not found")
	// ErrExists 记录已存在
	ErrExists = errors.New("store: already exists")
)

// Tx 事务句柄。事务内的读写只作用于工作副本，Commit 时才整体生效：
//   - Commit 成功：事务内的全部变更原子可见（FileStore 额外原子落盘一次）
//   - Rollback / 未 Commit：全部变更丢弃
//
// Service 层的每次业务操作（会话状态、交接单、审计、计数、消息归属等
// 多条记录的联动修改）都在单个事务内完成，因此进程在任意时刻崩溃，
// 重启后看到的要么是操作前的完整状态，要么是操作后的完整状态。
type Tx interface {
	Store
	// Commit 提交事务（整体生效）
	Commit() error
	// Rollback 放弃事务（整体不生效）
	Rollback()
}

// Store 持久化接口。所有写操作由 Service 层在同一把全局锁内串行调用，
// FileStore 以"全量快照原子写盘"方式保证多记录之间的事务一致性。
type Store interface {
	Init(ctx context.Context) error
	Close() error

	// BeginTx 开启一个事务（工作副本隔离，Commit 时整体生效）
	BeginTx(ctx context.Context) (Tx, error)

	PutSession(ctx context.Context, s *model.Session) error
	GetSession(ctx context.Context, id model.SessionID) (*model.Session, error)
	ListSessions(ctx context.Context) ([]*model.Session, error)

	PutAgent(ctx context.Context, a *model.Agent) error
	GetAgent(ctx context.Context, id model.AgentID) (*model.Agent, error)
	ListAgents(ctx context.Context) ([]*model.Agent, error)

	AppendMessage(ctx context.Context, m *model.Message) error
	UpdateMessage(ctx context.Context, m *model.Message) error
	ListMessages(ctx context.Context, sessionID model.SessionID) ([]*model.Message, error)

	PutHandoff(ctx context.Context, h *model.Handoff) error
	GetHandoff(ctx context.Context, id string) (*model.Handoff, error)
	ListHandoffs(ctx context.Context, sessionID model.SessionID) ([]*model.Handoff, error)

	AppendAudit(ctx context.Context, e *model.AuditEvent) error
	ListAudit(ctx context.Context, sessionID model.SessionID) ([]*model.AuditEvent, error)

	PutQueueEntry(ctx context.Context, q *model.QueueEntry) error
	DeleteQueueEntry(ctx context.Context, sessionID model.SessionID) error
	ListQueue(ctx context.Context) ([]*model.QueueEntry, error)
}
