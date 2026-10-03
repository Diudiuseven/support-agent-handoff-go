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

	if _, err := s.st.GetSession(ctx, id); err == nil {
		return nil, store.ErrExists
	} else if err != store.ErrNotFound {
		return nil, err
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
	if err := s.st.PutSession(ctx, sess); err != nil {
		return nil, err
	}
	s.auditLocked(ctx, id, "system", model.RoleSystem, "session.start", true, "skill="+skill)
	return sess, nil
}

// EndSession 结束会话，释放坐席容量
func (s *Service) EndSession(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
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
	if sess.State == model.StateAgent && sess.HandlerID != "" {
		if a, err := s.st.GetAgent(ctx, model.AgentID(sess.HandlerID)); err == nil {
			if a.Active > 0 {
				a.Active--
			}
			a.UpdatedAt = now
			if err := s.st.PutAgent(ctx, a); err != nil {
				return err
			}
		}
	}
	sess.State = model.StateEnded
	sess.Reason = model.ReasonSessionEnded
	sess.UpdatedAt = now
	sess.EndedAt = &now
	if err := s.st.PutSession(ctx, sess); err != nil {
		return err
	}
	_ = s.st.DeleteQueueEntry(ctx, sessionID)
	s.auditLocked(ctx, sessionID, actorID, role, "session.end", true, "")
	return nil
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
