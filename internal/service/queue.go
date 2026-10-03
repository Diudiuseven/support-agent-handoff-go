package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// dispatchLocked 执行一轮分发（调用方持有 s.mu）
func (s *Service) dispatchLocked(ctx context.Context) error {
	entries, err := s.st.ListQueue(ctx)
	if err != nil {
		return err
	}
	now := s.now()
	for _, q := range sortedQueue(entries) {
		sess, err := s.st.GetSession(ctx, q.SessionID)
		if err != nil {
			continue
		}
		if sess.State == model.StateEnded {
			_ = s.st.DeleteQueueEntry(ctx, q.SessionID)
			continue
		}
		// 会话已被接管/转接等，队列条目失效，丢弃
		if sess.State != model.StateQueued && sess.State != model.StateOffering {
			_ = s.st.DeleteQueueEntry(ctx, q.SessionID)
			continue
		}

		// 1) 排队总超时：按既定策略升级或回退机器人
		if now.After(q.QueueDeadline) {
			if err := s.applyTimeoutPolicy(ctx, sess, q, now); err != nil {
				return err
			}
			continue
		}

		// 2) 抢占邀请超时：重试或执行排队策略
		if q.OfferID != "" && now.After(q.OfferDeadline) {
			q.OfferID = ""
			sess.OfferID = ""
			if q.OfferCount > s.cfg.OfferRetries {
				if err := s.applyTimeoutPolicy(ctx, sess, q, now); err != nil {
					return err
				}
				continue
			}
			sess.State = model.StateQueued
			sess.Reason = model.ReasonOfferTimeout
			if err := s.st.PutSession(ctx, sess); err != nil {
				return err
			}
			if err := s.st.PutQueueEntry(ctx, q); err != nil {
				return err
			}
			s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "offer.timeout", true,
				fmt.Sprintf("offer retry %d, requeued", q.OfferCount))
		}

		// 3) 无有效邀请：尝试向可用坐席发起抢占
		if q.OfferID == "" {
			agents, err := s.eligibleAgents(ctx, q.Skill)
			if err != nil {
				return err
			}
			if len(agents) == 0 {
				continue // 坐席满载/不可用，等待上线或容量释放
			}
			offerID := newID("offer")
			q.OfferID = offerID
			q.OfferDeadline = now.Add(s.cfg.OfferTimeout)
			q.OfferCount++
			sess.State = model.StateOffering
			sess.OfferID = offerID
			sess.Reason = model.ReasonHandoffRequested
			if err := s.st.PutSession(ctx, sess); err != nil {
				return err
			}
			if err := s.st.PutQueueEntry(ctx, q); err != nil {
				return err
			}
			for _, a := range agents {
				s.notifyAgent(a.ID, &model.Event{
					Type:      model.EventOffer,
					SessionID: sess.ID,
					Text:      fmt.Sprintf("offer %s skill=%s priority=%d", offerID, q.Skill, q.Priority),
					At:        now,
				})
			}
			s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "offer.create", true,
				fmt.Sprintf("offer=%s agents=%d skill=%s", offerID, len(agents), q.Skill))
		}
	}
	return nil
}

// applyTimeoutPolicy 排队超时/邀请重试耗尽时的既定策略：
//   - escalate：升级到高级技能组（仅一次），提升优先级，继续排队
//   - fallback：回退机器人处理
func (s *Service) applyTimeoutPolicy(ctx context.Context, sess *model.Session, q *model.QueueEntry, now time.Time) error {
	policy := s.cfg.DefaultPolicy
	// 已升级过仍无坐席，则回退机器人，避免无限等待
	if policy == model.PolicyEscalate && !sess.Escalated {
		sess.Escalated = true
		sess.Skill = s.cfg.EscalationSkill
		sess.Priority++
		sess.State = model.StateQueued
		sess.HandlerType = ""
		sess.HandlerID = ""
		sess.OfferID = ""
		sess.Reason = model.ReasonEscalated
		q.Skill = s.cfg.EscalationSkill
		q.Priority = sess.Priority
		q.OfferID = ""
		q.OfferCount = 0
		q.QueueDeadline = now.Add(s.cfg.QueueTimeout)
		q.OfferDeadline = time.Time{}
		if err := s.st.PutSession(ctx, sess); err != nil {
			return err
		}
		if err := s.st.PutQueueEntry(ctx, q); err != nil {
			return err
		}
		s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "queue.timeout", true,
			fmt.Sprintf("escalate to skill=%s priority=%d", q.Skill, q.Priority))
		// 通知主管邮箱（若在线）
		if sups, err := s.findSupervisors(ctx); err == nil {
			for _, sup := range sups {
				s.notifyAgent(sup.ID, &model.Event{
					Type:      model.EventNotice,
					SessionID: sess.ID,
					Text:      fmt.Sprintf("session %s escalated, need senior agent", sess.ID),
					At:        now,
				})
			}
		}
		return nil
	}

	// fallback：回退机器人
	sess.State = model.StateBot
	sess.HandlerType = "bot"
	sess.HandlerID = ""
	sess.OfferID = ""
	sess.Reason = model.ReasonQueueTimeout
	if err := s.st.PutSession(ctx, sess); err != nil {
		return err
	}
	if err := s.st.DeleteQueueEntry(ctx, sess.ID); err != nil {
		return err
	}
	h := &model.Handoff{
		ID:          newID("handoff"),
		SessionID:   sess.ID,
		FromType:    "queue",
		FromID:      "",
		ToType:      "bot",
		ToID:        "",
		Reason:      model.ReasonQueueTimeout,
		Status:      model.HandoffCompleted,
		CreatedAt:   now,
		CompletedAt: &now,
	}
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return err
	}
	s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "queue.timeout", true,
		"fallback to bot")
	return nil
}

func (s *Service) findSupervisors(ctx context.Context) ([]*model.Agent, error) {
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	var out []*model.Agent
	for _, a := range agents {
		if a.Role == model.RoleSupervisor && a.Online {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *Service) ListQueue(ctx context.Context) ([]*model.QueueEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListQueue(ctx)
}
