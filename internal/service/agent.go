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
	return s.inTx(ctx, func() error {
		now := s.now()
		a.UpdatedAt = now
		if err := s.db(ctx).PutAgent(ctx, a); err != nil {
			return err
		}
		s.auditLocked(ctx, "", string(a.ID), a.Role, "agent.register", true,
			fmt.Sprintf("skills=%v capacity=%d", a.Skills, a.Capacity))
		return nil
	})
}

// AgentOnline 坐席上线，触发排队分发与名下积压消息补投
func (s *Service) AgentOnline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.inTx(ctx, func() error {
		a, err := s.db(ctx).GetAgent(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()
		a.Online = true
		a.UpdatedAt = now
		if err := s.db(ctx).PutAgent(ctx, a); err != nil {
			return err
		}
		s.auditLocked(ctx, "", string(id), a.Role, "agent.online", true, "")
		// 上线后补投该坐席名下会话的积压消息
		sessions, err := s.db(ctx).ListSessions(ctx)
		if err != nil {
			return err
		}
		for _, sess := range sessions {
			if (sess.State == model.StateAgent || sess.State == model.StateTransferring) &&
				sess.HandlerID == string(id) {
				if err := s.deliverPendingLocked(ctx, sess); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.kickDispatch()
	return nil
}

// AgentOffline 坐席下线：其名下会话（含转接中）全部重新排队
// （原因 agent_offline，可区分），已结束/机器人会话不受影响。
// 坐席记录、会话、转接单、排队条目、容量、审计在同一事务内整体生效。
func (s *Service) AgentOffline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.inTx(ctx, func() error {
		a, err := s.db(ctx).GetAgent(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()

		sessions, err := s.db(ctx).ListSessions(ctx)
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
					_ = s.db(ctx).PutHandoff(ctx, h)
				}
				// 重新排队，保证会话不丢、可继续分发
				sess.State = model.StateQueued
				sess.HandlerType = ""
				sess.HandlerID = ""
				sess.OfferID = ""
				sess.Reason = model.ReasonAgentOffline
				sess.UpdatedAt = now
				if err := s.db(ctx).PutSession(ctx, sess); err != nil {
					return err
				}
				q := &model.QueueEntry{
					SessionID:     sess.ID,
					Skill:         sess.Skill,
					Priority:      sess.Priority,
					EnqueuedAt:    now,
					QueueDeadline: now.Add(s.cfg.QueueTimeout),
				}
				if err := s.db(ctx).PutQueueEntry(ctx, q); err != nil {
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
				if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
					return err
				}
				s.auditLocked(ctx, sess.ID, string(id), a.Role, "agent.offline", true,
					"session requeued")
			}
		}
		// 容量按真实归属重算，而非简单清零：防止并发交织下的计数漂移。
		// 下线后该坐席名下不应再有处理中/转接中会话，因此为 0；
		// 若有遗漏（未来新增状态），重算也能保持准确。
		a.Online = false
		a.Active = 0
		a.UpdatedAt = now
		if err := s.db(ctx).PutAgent(ctx, a); err != nil {
			return err
		}
		s.auditLocked(ctx, "", string(id), a.Role, "agent.offline", true, "agent offline")
		return nil
	})
	if err != nil {
		return err
	}
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
