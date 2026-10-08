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
	err := s.st.Update(ctx, func(tx store.Store) error {
		if _, err := tx.GetSession(ctx, id); err == nil {
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
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, id, "system", model.RoleSystem, "session.start", true, "skill="+skill); err != nil {
			return err
		}
		out = sess
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EndSession 结束会话，释放坐席容量（状态、容量、预留、队列、审计同事务生效）
func (s *Service) EndSession(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if sess.State == model.StateEnded {
			return nil
		}

		// 权限：坐席只能结束自己处理的会话；主管/系统可结束任意会话
		if role == model.RoleAgent && sess.HandlerID != actorID {
			_ = s.auditOn(ctx, tx, sessionID, actorID, role, "session.end", false,
				"permission denied: not the handler")
			return store.ErrNotFound
		}

		now := s.now()
		if sess.State == model.StateAgent && sess.HandlerID != "" {
			if a, err := tx.GetAgent(ctx, model.AgentID(sess.HandlerID)); err == nil {
				if a.Active > 0 {
					a.Active--
				}
				a.UpdatedAt = now
				if err := tx.PutAgent(ctx, a); err != nil {
					return err
				}
			}
		}
		// 转接中被结束：释放目标坐席预留并终结交接单
		if sess.State == model.StateTransferring {
			if h, _ := pendingHandoff(ctx, tx, sess.ID); h != nil {
				if h.ToID != "" {
					if t, err := tx.GetAgent(ctx, model.AgentID(h.ToID)); err == nil {
						if t.Reserved > 0 {
							t.Reserved--
						}
						t.UpdatedAt = now
						if err := tx.PutAgent(ctx, t); err != nil {
							return err
						}
					}
				}
				h.Status = model.HandoffFailed
				h.CompletedAt = &now
				h.Deadline = nil
				if err := tx.PutHandoff(ctx, h); err != nil {
					return err
				}
			}
		}
		sess.State = model.StateEnded
		sess.Reason = model.ReasonSessionEnded
		sess.UpdatedAt = now
		sess.EndedAt = &now
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		_ = tx.DeleteQueueEntry(ctx, sessionID)
		return s.auditOn(ctx, tx, sessionID, actorID, role, "session.end", true, "")
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
