package service

import (
	"context"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// StartSession 创建一个机器人处理中的会话
func (s *Service) StartSession(ctx context.Context, id model.SessionID, skill string) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Session
	err := s.inTx(ctx, func() error {
		if _, err := s.db(ctx).GetSession(ctx, id); err == nil {
			return store.ErrExists
		} else if err != store.ErrNotFound {
			return err
		}

		now := s.now()
		sess := &model.Session{
			ID:          id,
			State:       model.StateBot,
			HandlerType: "bot",
			Skill:       skill,
			Reason:      model.ReasonHandoffRequested,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
			return err
		}
		s.auditLocked(ctx, id, "system", model.RoleSystem, "session.start", true, "skill="+skill)
		out = sess
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EndSession 结束会话，释放坐席容量。
// 会话状态、容量、队列清理、审计在同一事务内整体生效。
func (s *Service) EndSession(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if sess.State == model.StateEnded {
			return nil
		}

		// 权限：坐席只能结束自己处理的会话；主管/系统可结束任意会话
		if role == model.RoleAgent && sess.HandlerID != actorID {
			s.auditLocked(ctx, sessionID, actorID, role, "session.end", false,
				"permission denied: not the handler")
			return store.ErrNotFound
		}

		now := s.now()
		if (sess.State == model.StateAgent || sess.State == model.StateTransferring) && sess.HandlerID != "" {
			if a, err := s.db(ctx).GetAgent(ctx, model.AgentID(sess.HandlerID)); err == nil {
				// 转接中结束：只有原坐席是处理方，目标未占用，故只释放原坐席
				if a.Active > 0 {
					a.Active--
				}
				a.UpdatedAt = now
				if err := s.db(ctx).PutAgent(ctx, a); err != nil {
					return err
				}
			}
			// 若存在未完成转接单，置失败
			if h, _ := s.pendingHandoffLocked(ctx, sessionID); h != nil {
				h.Status = model.HandoffFailed
				h.CompletedAt = &now
				if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
					return err
				}
			}
		}
		sess.State = model.StateEnded
		sess.Reason = model.ReasonSessionEnded
		sess.UpdatedAt = now
		sess.EndedAt = &now
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
			return err
		}
		_ = s.db(ctx).DeleteQueueEntry(ctx, sessionID)
		s.auditLocked(ctx, sessionID, actorID, role, "session.end", true, "")
		return nil
	})
}

func (s *Service) GetSession(ctx context.Context, id model.SessionID) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.GetSession(ctx, id)
}

func (s *Service) ListSessions(ctx context.Context) ([]*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListSessions(ctx)
}
