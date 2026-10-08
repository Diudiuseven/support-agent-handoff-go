package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// dispatchLocked 执行一轮分发（调用方持有 s.mu）：
//  1. 先终结所有已超时的未完成转接（确定性兜底，杜绝卡死）；
//  2. 再推进排队队列（超时策略、邀请重发）。
func (s *Service) dispatchLocked(ctx context.Context) error {
	if err := s.sweepExpiredTransfersLocked(ctx); err != nil {
		return err
	}
	entries, err := s.st.ListQueue(ctx)
	if err != nil {
		return err
	}
	now := s.now()
	for _, q := range sortedQueue(entries) {
		if err := s.dispatchOneLocked(ctx, q, now); err != nil {
			return err
		}
	}
	return nil
}

// sweepExpiredTransfersLocked 扫描转接中的会话：超过交接单 Deadline 仍未被
// 接受/拒绝的，原坐席在线则明确回到原坐席，原坐席离线则重新排队，
// 原因 transfer_timeout 可区分。保证转接中间态存在上界，不会长期卡死。
func (s *Service) sweepExpiredTransfersLocked(ctx context.Context) error {
	sessions, err := s.st.ListSessions(ctx)
	if err != nil {
		return err
	}
	now := s.now()
	var changed []*model.Session
	for _, sess := range sessions {
		if sess.State != model.StateTransferring {
			continue
		}
		h, err := s.pendingHandoffLocked(ctx, sess.ID)
		if err != nil {
			return err
		}
		if h == nil || h.Deadline == nil || !now.After(*h.Deadline) {
			continue
		}
		err = s.st.Update(ctx, func(tx store.Store) error {
			hh, err := pendingHandoff(ctx, tx, sess.ID)
			if err != nil {
				return err
			}
			if hh == nil {
				return nil
			}
			ss, err := tx.GetSession(ctx, sess.ID)
			if err != nil {
				return err
			}
			if ss.State != model.StateTransferring {
				return nil
			}
			if err := s.finalizeTransferFailure(ctx, tx, ss, hh, model.ReasonTransferTimeout, now); err != nil {
				return err
			}
			changed = append(changed, ss)
			return nil
		})
		if err != nil {
			return err
		}
	}
	for _, sess := range changed {
		s.afterRoutingChanged(ctx, sess)
	}
	return nil
}

// dispatchOneLocked 在一个事务内推进单个排队条目
func (s *Service) dispatchOneLocked(ctx context.Context, q *model.QueueEntry, now time.Time) error {
	var offer []model.AgentID // 本轮新邀请需通知的坐席（事务外发送）
	var offerInfo string

	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, q.SessionID)
		if err != nil {
			return nil // 会话已不存在，丢弃条目
		}
		if sess.State == model.StateEnded {
			return tx.DeleteQueueEntry(ctx, q.SessionID)
		}
		// 会话已被接管/转接等，队列条目失效，丢弃
		if sess.State != model.StateQueued && sess.State != model.StateOffering {
			return tx.DeleteQueueEntry(ctx, q.SessionID)
		}

		// 重新读取条目，避免使用过期快照
		var entry *model.QueueEntry
		if cur, err := tx.ListQueue(ctx); err == nil {
			for _, e := range cur {
				if e.SessionID == q.SessionID {
					entry = e
				}
			}
		}
		if entry == nil {
			return nil
		}
		q = entry

		// 1) 排队总超时：按既定策略升级或回退机器人
		if now.After(q.QueueDeadline) {
			return s.applyTimeoutPolicy(ctx, tx, sess, q, now)
		}

		// 2) 抢占邀请超时：重试或执行排队策略
		if q.OfferID != "" && now.After(q.OfferDeadline) {
			q.OfferID = ""
			sess.OfferID = ""
			if q.OfferCount > s.cfg.OfferRetries {
				return s.applyTimeoutPolicy(ctx, tx, sess, q, now)
			}
			sess.State = model.StateQueued
			sess.Reason = model.ReasonOfferTimeout
			if err := tx.PutSession(ctx, sess); err != nil {
				return err
			}
			if err := tx.PutQueueEntry(ctx, q); err != nil {
				return err
			}
			return s.auditOn(ctx, tx, sess.ID, "system", model.RoleSystem, "offer.timeout", true,
				fmt.Sprintf("offer retry %d, requeued", q.OfferCount))
		}

		// 3) 无有效邀请：尝试向可用坐席发起抢占
		if q.OfferID == "" {
			agents, err := eligibleAgents(ctx, tx, q.Skill)
			if err != nil {
				return err
			}
			if len(agents) == 0 {
				return nil // 坐席满载/不可用，等待上线或容量释放
			}
			offerID := newID("offer")
			q.OfferID = offerID
			q.OfferDeadline = now.Add(s.cfg.OfferTimeout)
			q.OfferCount++
			sess.State = model.StateOffering
			sess.OfferID = offerID
			sess.Reason = model.ReasonHandoffRequested
			if err := tx.PutSession(ctx, sess); err != nil {
				return err
			}
			if err := tx.PutQueueEntry(ctx, q); err != nil {
				return err
			}
			if err := s.auditOn(ctx, tx, sess.ID, "system", model.RoleSystem, "offer.create", true,
				fmt.Sprintf("offer=%s agents=%d skill=%s", offerID, len(agents), q.Skill)); err != nil {
				return err
			}
			for _, a := range agents {
				id := a.ID
				offer = append(offer, id)
			}
			offerInfo = fmt.Sprintf("offer %s skill=%s priority=%d", offerID, q.Skill, q.Priority)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range offer {
		s.notifyAgent(id, &model.Event{
			Type:      model.EventOffer,
			SessionID: q.SessionID,
			Text:      offerInfo,
			At:        now,
		})
	}
	return nil
}

// applyTimeoutPolicy 排队超时/邀请重试耗尽时的既定策略（在给定事务内执行）：
//   - escalate：升级到高级技能组（仅一次），提升优先级，继续排队
//   - fallback：回退机器人处理
func (s *Service) applyTimeoutPolicy(ctx context.Context, tx store.Store, sess *model.Session, q *model.QueueEntry, now time.Time) error {
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
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		if err := tx.PutQueueEntry(ctx, q); err != nil {
			return err
		}
		return s.auditOn(ctx, tx, sess.ID, "system", model.RoleSystem, "queue.timeout", true,
			fmt.Sprintf("escalate to skill=%s priority=%d", q.Skill, q.Priority))
	}

	// fallback：回退机器人
	sess.State = model.StateBot
	sess.HandlerType = "bot"
	sess.HandlerID = ""
	sess.OfferID = ""
	sess.Reason = model.ReasonQueueTimeout
	if err := tx.PutSession(ctx, sess); err != nil {
		return err
	}
	if err := tx.DeleteQueueEntry(ctx, sess.ID); err != nil {
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
	if err := tx.PutHandoff(ctx, h); err != nil {
		return err
	}
	return s.auditOn(ctx, tx, sess.ID, "system", model.RoleSystem, "queue.timeout", true,
		"fallback to bot")
}

// eligibleAgents 在指定事务视图上返回可承接该技能的在线有余量坐席
func eligibleAgents(ctx context.Context, st2 store.Store, skill string) ([]*model.Agent, error) {
	agents, err := st2.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	var out []*model.Agent
	for _, a := range agents {
		if !a.Online || a.Active+a.Reserved >= a.Capacity {
			continue
		}
		if a.Role == model.RoleSupervisor {
			out = append(out, a)
			continue
		}
		for _, sk := range a.Skills {
			if sk == skill {
				out = append(out, a)
				break
			}
		}
	}
	return out, nil
}

func (s *Service) ListQueue(ctx context.Context) ([]*model.QueueEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListQueue(ctx)
}
