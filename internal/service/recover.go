package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// Recover 重启恢复（由 Run 在后台循环启动前调用）。整个恢复在单个事务内完成：
// 进程即使在恢复过程中再次被终止，下一次重启仍会从头幂等执行，不留半完成态。
//
//   - offering  → queued（在途邀请随进程消失，重新分发）
//   - transferring：未完成转接单失败，回退原坐席（在线）或重新排队（下线）
//   - agent 但交接单 pending：补完成（状态切换与单据落盘之间崩溃）；
//     缺少交接记录则补登，保证可追溯
//   - queued：补齐排队条目，重新计时
//   - 按会话真实归属重算所有坐席 Active（修正计数漂移），绝不抬高 Capacity
//   - 在线坐席名下积压消息保持 Delivered=false，由补投循环/上线继续送达
//
// 全程幂等：重复恢复不产生重复单据、重复排队条目或重复计数。
func (s *Service) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.inTx(ctx, func() error {
		now := s.now()
		sessions, err := s.db(ctx).ListSessions(ctx)
		if err != nil {
			return err
		}
		queues, err := s.db(ctx).ListQueue(ctx)
		if err != nil {
			return err
		}
		qBySession := make(map[model.SessionID]*model.QueueEntry, len(queues))
		for _, q := range queues {
			qBySession[q.SessionID] = q
		}

		for _, sess := range sessions {
			switch sess.State {
			case model.StateEnded, model.StateBot:
				if _, ok := qBySession[sess.ID]; ok {
					_ = s.db(ctx).DeleteQueueEntry(ctx, sess.ID)
				}
				continue

			case model.StateQueued:
				if _, ok := qBySession[sess.ID]; !ok {
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
				}

			case model.StateOffering:
				sess.State = model.StateQueued
				sess.OfferID = ""
				sess.Reason = model.ReasonRecovered
				sess.UpdatedAt = now
				if err := s.db(ctx).PutSession(ctx, sess); err != nil {
					return err
				}
				if q, ok := qBySession[sess.ID]; ok {
					q.OfferID = ""
					q.OfferDeadline = time.Time{}
					if err := s.db(ctx).PutQueueEntry(ctx, q); err != nil {
						return err
					}
				} else {
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
				}
				s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
					"offering -> queued")

			case model.StateTransferring:
				// 未完成的转接：同"超时裁决"——回退原坐席或重新排队
				h, _ := s.pendingHandoffLocked(ctx, sess.ID)
				if h == nil {
					if err := s.requeueSessionLocked(ctx, sess, model.ReasonRecovered); err != nil {
						return err
					}
					s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
						"transferring without handoff -> queued")
					continue
				}
				if err := s.resolveFailedTransferLocked(ctx, sess, h, model.ReasonRecovered); err != nil {
					return err
				}
				s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
					"transferring -> resolved")

			case model.StateAgent:
				// 恢复过程中状态可能已被前序步骤改写，重新读取最新会话
				cur, gerr := s.db(ctx).GetSession(ctx, sess.ID)
				if gerr != nil {
					return gerr
				}
				if cur.State != model.StateAgent {
					continue // 已被转为排队等，跳过，避免覆盖
				}
				sess = cur
				// 补完成未交接单；若缺少当前处理方的交接记录则补登，保证可追溯
				h, _ := s.pendingHandoffLocked(ctx, sess.ID)
				if h != nil {
					if h.ToID == sess.HandlerID {
						h.Status = model.HandoffCompleted
						h.CompletedAt = &now
						_ = s.db(ctx).PutHandoff(ctx, h)
						s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
							fmt.Sprintf("pending handoff completed for %s", sess.HandlerID))
					} else {
						// 会话归属与 pending 单目标不一致：单据失败，会话归属以状态为准
						h.Status = model.HandoffFailed
						h.CompletedAt = &now
						_ = s.db(ctx).PutHandoff(ctx, h)
						s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", false,
							fmt.Sprintf("pending handoff target %s != handler %s", h.ToID, sess.HandlerID))
					}
				} else if !s.hasCompletedHandoff(ctx, sess.ID, sess.HandlerID) {
					record := &model.Handoff{
						ID:          newID("handoff"),
						SessionID:   sess.ID,
						FromType:    "queue",
						ToType:      "agent",
						ToID:        sess.HandlerID,
						Reason:      model.ReasonRecovered,
						Status:      model.HandoffCompleted,
						CreatedAt:   sess.UpdatedAt,
						CompletedAt: &now,
					}
					_ = s.db(ctx).PutHandoff(ctx, record)
					s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
						fmt.Sprintf("handoff record rebuilt for %s", sess.HandlerID))
				}
				_ = s.db(ctx).DeleteQueueEntry(ctx, sess.ID)
			}
		}

		// 按会话真实归属重算所有坐席 Active，修正崩溃造成的计数漂移。
		// 只修正 Active，绝不修改 Capacity（容量不能被超卖，也不能被漂移）。
		agents, err := s.db(ctx).ListAgents(ctx)
		if err != nil {
			return err
		}
		activeCount := make(map[model.AgentID]int)
		for _, sess := range sessions {
			if (sess.State == model.StateAgent || sess.State == model.StateTransferring) && sess.HandlerID != "" {
				activeCount[model.AgentID(sess.HandlerID)]++
			}
		}
		for _, a := range agents {
			a.Active = activeCount[a.ID]
			a.UpdatedAt = now
			if err := s.db(ctx).PutAgent(ctx, a); err != nil {
				return err
			}
		}

		// 在线坐席名下的积压消息补投（事件在事务提交后发出）
		fresh, err := s.db(ctx).ListSessions(ctx)
		if err != nil {
			return err
		}
		for _, sess := range fresh {
			if sess.State != model.StateAgent || sess.HandlerID == "" {
				continue
			}
			if a, err := s.db(ctx).GetAgent(ctx, model.AgentID(sess.HandlerID)); err == nil && a.Online {
				if err := s.deliverPendingLocked(ctx, sess); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// hasCompletedHandoff 判断是否已存在指向 toID 的完成交接单，避免恢复时重复补登
func (s *Service) hasCompletedHandoff(ctx context.Context, sessionID model.SessionID, toID string) bool {
	list, err := s.db(ctx).ListHandoffs(ctx, sessionID)
	if err != nil {
		return false
	}
	for _, h := range list {
		if h.Status == model.HandoffCompleted && h.ToID == toID {
			return true
		}
	}
	return false
}
