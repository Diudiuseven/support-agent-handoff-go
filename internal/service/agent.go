package service

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
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
	if err := s.st.Update(ctx, func(tx store.Store) error {
		if err := tx.PutAgent(ctx, a); err != nil {
			return err
		}
		return s.auditOn(ctx, tx, "", string(a.ID), a.Role, "agent.register", true,
			fmt.Sprintf("skills=%v capacity=%d", a.Skills, a.Capacity))
	}); err != nil {
		return err
	}
	return nil
}

// AgentOnline 坐席上线，触发排队分发
func (s *Service) AgentOnline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var handled []*model.Session
	if err := s.st.Update(ctx, func(tx store.Store) error {
		a, err := tx.GetAgent(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()
		a.Online = true
		a.UpdatedAt = now
		if err := tx.PutAgent(ctx, a); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, "", string(id), a.Role, "agent.online", true, ""); err != nil {
			return err
		}
		sessions, err := tx.ListSessions(ctx)
		if err != nil {
			return err
		}
		for _, sess := range sessions {
			if sess.State == model.StateAgent && sess.HandlerID == string(id) {
				handled = append(handled, sess)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// 上线后补投该坐席名下会话的积压消息（事务外，非阻塞，周期循环也会兜底）
	for _, sess := range handled {
		_ = s.deliverPendingLocked(ctx, sess)
	}
	s.kickDispatch()
	return nil
}

// AgentOffline 坐席下线：其名下会话重新排队（原因 agent_offline，可区分），
// 已结束/机器人会话不受影响；作为转接目标被预留的容量一并释放。
// 每个会话的状态、计数、排队条目、交接单、审计在同一事务内整体生效。
func (s *Service) AgentOffline(ctx context.Context, id model.AgentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var affected []*model.Session
	handled := make(map[string]bool) // 已终结的转接单，防止跨循环重复释放预留
	err := s.st.Update(ctx, func(tx store.Store) error {
		a, err := tx.GetAgent(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()
		a.Online = false
		a.UpdatedAt = now

		sessions, err := tx.ListSessions(ctx)
		if err != nil {
			return err
		}
		// 先释放所有"以该坐席为目标"的转接预留（它无法再接受任何转接）
		for _, sess := range sessions {
			if sess.State != model.StateTransferring {
				continue
			}
			h, err := pendingHandoff(ctx, tx, sess.ID)
			if err != nil {
				return err
			}
			if h == nil || h.ToID != string(id) {
				continue
			}
			if a.Reserved > 0 {
				a.Reserved--
			}
			h.Status = model.HandoffFailed
			h.CompletedAt = &now
			h.Deadline = nil
			if err := tx.PutHandoff(ctx, h); err != nil {
				return err
			}
			handled[h.ID] = true
			// 会话处理方是别的坐席：路由回原坐席或重新排队
			if sess.HandlerID != string(id) {
				if _, err := s.routeBackFromTransfer(ctx, tx, sess, model.ReasonTransferTargetGone, now); err != nil {
					return err
				}
				if err := s.auditOn(ctx, tx, sess.ID, string(id), a.Role, "transfer.finalize", true,
					"target went offline before accept"); err != nil {
					return err
				}
				affected = append(affected, sess)
			}
		}

		for _, sess := range sessions {
			if sess.HandlerID != string(id) {
				continue
			}
			switch sess.State {
			case model.StateAgent, model.StateTransferring:
				// 转接中的未完成交接单置失败，并释放目标预留
				if h, _ := pendingHandoff(ctx, tx, sess.ID); h != nil && !handled[h.ID] {
					h.Status = model.HandoffFailed
					h.CompletedAt = &now
					h.Deadline = nil
					if err := tx.PutHandoff(ctx, h); err != nil {
						return err
					}
					if h.ToID != "" {
						if tgt, err := tx.GetAgent(ctx, model.AgentID(h.ToID)); err == nil {
							if tgt.Reserved > 0 {
								tgt.Reserved--
							}
							tgt.UpdatedAt = now
							if err := tx.PutAgent(ctx, tgt); err != nil {
								return err
							}
						}
					}
				}
				// 重新排队，保证会话不丢、可继续分发
				sess.State = model.StateQueued
				sess.HandlerType = ""
				sess.HandlerID = ""
				sess.OfferID = ""
				sess.Reason = model.ReasonAgentOffline
				sess.UpdatedAt = now
				if err := tx.PutSession(ctx, sess); err != nil {
					return err
				}
				q := &model.QueueEntry{
					SessionID:     sess.ID,
					Skill:         sess.Skill,
					Priority:      sess.Priority,
					EnqueuedAt:    now,
					QueueDeadline: now.Add(s.cfg.QueueTimeout),
				}
				if err := tx.PutQueueEntry(ctx, q); err != nil {
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
				if err := tx.PutHandoff(ctx, h); err != nil {
					return err
				}
				if err := s.auditOn(ctx, tx, sess.ID, string(id), a.Role, "agent.offline", true,
					"session requeued"); err != nil {
					return err
				}
				affected = append(affected, sess)
			}
		}
		// 下线坐席不再占用任何容量
		a.Active = 0
		a.Reserved = 0
		if err := tx.PutAgent(ctx, a); err != nil {
			return err
		}
		return s.auditOn(ctx, tx, "", string(id), a.Role, "agent.offline", true, "agent offline")
	})
	if err != nil {
		return err
	}
	for _, sess := range affected {
		s.afterRoutingChanged(ctx, sess)
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
		if a.Active+a.Reserved >= a.Capacity {
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
