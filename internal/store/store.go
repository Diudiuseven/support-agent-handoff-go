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

// Store 持久化接口。所有写操作由 Service 层在同一把全局锁内串行调用，
// FileStore 以"全量快照原子写盘"方式保证多记录之间的事务一致性。
type Store interface {
	Init(ctx context.Context) error
	Close() error

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

	// Update 在一个事务内执行批量状态变更。fn 在事务视图 tx 上操作：
	//   - fn 返回 nil：全部变更整体生效（FileStore 下为单次原子快照落盘）；
	//   - fn 返回错误：全部变更整体回滚，持久化状态保持事务前不变。
	// 调用方不得在 fn 内再对原 Store 发起写操作（会与事务锁形成嵌套）。
	Update(ctx context.Context, fn func(tx Store) error) error
}
