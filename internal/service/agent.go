package service

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// RegisterAgent 注册坐席
func (s *Service) RegisterAgent(ctx context.Context, a *model.Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if a.ID == "" {
		return fmt.Errorf("agent id required")
	}
	if a.Capacity <= 0 {
		a.Capacity = s.cfg.DefaultCapacity
	}
	if a.Role == "" {
		a.Role = model.RoleAgent
	}
	now := s.now()
	a.UpdatedAt = now
	if err := s.st.PutAgent(ctx, a); err != nil {
		return err
	}
	s.auditLocked(ctx, "", string(a.ID), a.Role, "agent.register", true,
		fmt.Sprintf("skills=%v capacity=%d", a.Skills, a.Capacity))
	return nil
}

// AgentOnline 坐席上线，触发排队分发
func (s *Service) AgentOnline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, err := s.st.GetAgent(ctx, id)
	if err != nil {
		return err
	}
	now := s.now()
	a.Online = true
	a.UpdatedAt = now
	if err := s.st.PutAgent(ctx, a); err != nil {
		return err
	}
	s.auditLocked(ctx, "", string(id), a.Role, "agent.online", true, "")
	// 上线后补投该坐席名下会话的积压消息
	if sessions, err := s.st.ListSessions(ctx); err == nil {
		for _, sess := range sessions {
			if sess.State == model.StateAgent && sess.HandlerID == string(id) {
				_ = s.deliverPendingLocked(ctx, sess)
			}
		}
	}
	s.kickDispatch()
	return nil
}

// AgentOffline 坐席下线：其名下会话重新排队（原因 agent_offline，可区分），
// 已结束/机器人会话不受影响
func (s *Service) AgentOffline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, err := s.st.GetAgent(ctx, id)
	if err != nil {
		return err
	}
	now := s.now()
	a.Online = false
	a.UpdatedAt = now

	sessions, err := s.st.ListSessions(ctx)
	if err != nil {
		return err
	}
	for _, sess := range sessions {
		if sess.HandlerID != string(id) {
			continue
		}
		switch sess.State {
		case model.StateAgent, model.StateTransferring:
			// 转接中的未完成交接单置失败
			if h, _ := s.pendingHandoffLocked(ctx, sess.ID); h != nil {
				h.Status = model.HandoffFailed
				h.CompletedAt = &now
				_ = s.st.PutHandoff(ctx, h)
			}
			// 重新排队，保证会话不丢、可继续分发
			sess.State = model.StateQueued
			sess.HandlerType = ""
			sess.HandlerID = ""
			sess.OfferID = ""
			sess.Reason = model.ReasonAgentOffline
			sess.UpdatedAt = now
			if err := s.st.PutSession(ctx, sess); err != nil {
				return err
			}
			q := &model.QueueEntry{
				SessionID:     sess.ID,
				Skill:         sess.Skill,
				Priority:      sess.Priority,
				EnqueuedAt:    now,
				QueueDeadline: now.Add(s.cfg.QueueTimeout),
			}
			if err := s.st.PutQueueEntry(ctx, q); err != nil {
				return err
			}
			h := &model.Handoff{
				ID:          newID("handoff"),
				SessionID:   sess.ID,
				FromType:    "agent",
				FromID:      string(id),
				ToType:      "queue",
				Reason:      model.ReasonAgentOffline,
				Status:      model.HandoffCompleted,
				CreatedAt:   now,
				CompletedAt: &now,
			}
			if err := s.st.PutHandoff(ctx, h); err != nil {
				return err
			}
			s.auditLocked(ctx, sess.ID, string(id), a.Role, "agent.offline", true,
				"session requeued")
		}
	}
	a.Active = 0
	if err := s.st.PutAgent(ctx, a); err != nil {
		return err
	}
	s.auditLocked(ctx, "", string(id), a.Role, "agent.offline", true, "agent offline")
	s.kickDispatch()
	return nil
}

// ListOffers 返回当前对该坐席有效的抢占邀请
func (s *Service) ListOffers(ctx context.Context, agentID model.AgentID) ([]*model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, err := s.st.GetAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	if !a.Online {
		return nil, nil
	}
	sessions, err := s.st.ListSessions(ctx)
	if err != nil {
		return nil, err
	}
	var out []*model.Event
	for _, sess := range sessions {
		if sess.State != model.StateOffering || sess.OfferID == "" {
			continue
		}
		if a.Active >= a.Capacity {
			break
		}
		if a.Role != model.RoleSupervisor {
			ok := false
			for _, sk := range a.Skills {
				if sk == sess.Skill {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		out = append(out, &model.Event{
			Type:      model.EventOffer,
			SessionID: sess.ID,
			Text:      fmt.Sprintf("offer %s", sess.OfferID),
			At:        s.now(),
		})
	}
	return out, nil
}

// Events 返回坐席邮箱通道（消息/邀请/通知）
func (s *Service) Events(ctx context.Context, agentID model.AgentID) (<-chan *model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.st.GetAgent(ctx, agentID); err != nil {
		return nil, err
	}
	return s.mailboxFor(agentID), nil
}

func (s *Service) ListAgents(ctx context.Context) ([]*model.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListAgents(ctx)
}
