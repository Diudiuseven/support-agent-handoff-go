package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// Auditor 只追加审计事件，所有写操作与业务状态变更在同一临界区内完成，
// 保证"操作发生"与"审计记录"一致。
type Auditor struct {
	st store.Store
}

func New(st store.Store) *Auditor {
	return &Auditor{st: st}
}

// Record 记录一条审计事件。allowed=false 表示越权/冲突被拒绝。
func (a *Auditor) Record(ctx context.Context, e *model.AuditEvent) error {
	if e.ID == "" {
		e.ID = fmt.Sprintf("audit-%d", time.Now().UnixNano())
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	return a.st.AppendAudit(ctx, e)
}

// 便捷构造函数
func Event(sessionID model.SessionID, actorID string, role model.Role, action string, allowed bool, detail string) *model.AuditEvent {
	return &model.AuditEvent{
		SessionID: sessionID,
		ActorID:   actorID,
		ActorRole: role,
		Action:    action,
		Allowed:   allowed,
		Detail:    detail,
	}
}

func (a *Auditor) List(ctx context.Context, sessionID model.SessionID) ([]*model.AuditEvent, error) {
	return a.st.ListAudit(ctx, sessionID)
}
