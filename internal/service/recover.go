package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// Recover 重启恢复（调用方为 Run，此时后台循环尚未启动）：
//   - offering/transferring 状态：邀请/转接在崩溃中丢失，回退排队继续分发
//   - agent 状态但交接单未完成：补完成交接单（状态切换与单据落盘之间崩溃）
//   - queued 状态：补齐排队条目，保留剩余超时时间，继续推进
//   - 重算坐席容量，修正计数漂移
//
// 全程幂等，重复恢复不产生重复处理。
func (s *Service) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	sessions, err := s.st.ListSessions(ctx)
	if err != nil {
		return err
	}
	queues, err := s.st.ListQueue(ctx)
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
				_ = s.st.DeleteQueueEntry(ctx, sess.ID)
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
				if err := s.st.PutQueueEntry(ctx, q); err != nil {
					return err
				}
			}

		case model.StateOffering:
			sess.State = model.StateQueued
			sess.OfferID = ""
			sess.Reason = model.ReasonRecovered
			sess.UpdatedAt = now
			if err := s.st.PutSession(ctx, sess); err != nil {
				return err
			}
			if q, ok := qBySession[sess.ID]; ok {
				q.OfferID = ""
				q.OfferDeadline = time.Time{}
				if err := s.st.PutQueueEntry(ctx, q); err != nil {
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
				if err := s.st.PutQueueEntry(ctx, q); err != nil {
					return err
				}
			}
			s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
				"offering -> queued")

		case model.StateTransferring:
			// 未完成的转接：作失败处理，会话回退排队
			if h, _ := s.pendingHandoffLocked(ctx, sess.ID); h != nil {
				h.Status = model.HandoffFailed
				h.CompletedAt = &now
				_ = s.st.PutHandoff(ctx, h)
			}
			sess.State = model.StateQueued
			sess.HandlerType = ""
			sess.HandlerID = ""
			sess.OfferID = ""
			sess.Reason = model.ReasonRecovered
			sess.UpdatedAt = now
			if err := s.st.PutSession(ctx, sess); err != nil {
				return err
			}
			if _, ok := qBySession[sess.ID]; !ok {
				q := &model.QueueEntry{
					SessionID:     sess.ID,
					Skill:         sess.Skill,
					Priority:      sess.Priority,
					EnqueuedAt:    now,
					QueueDeadline: now.Add(s.cfg.QueueTimeout),
				}
				_ = s.st.PutQueueEntry(ctx, q)
			}
			s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
				"transferring -> queued")

		case model.StateAgent:
			// 补完成未交接单；若缺少当前处理方的交接记录则补登，保证可追溯
			h, _ := s.pendingHandoffLocked(ctx, sess.ID)
			if h != nil {
				if h.ToID == sess.HandlerID {
					h.Status = model.HandoffCompleted
					h.CompletedAt = &now
					_ = s.st.PutHandoff(ctx, h)
					s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
						fmt.Sprintf("pending handoff completed for %s", sess.HandlerID))
				} else {
					h.Status = model.HandoffFailed
					h.CompletedAt = &now
					_ = s.st.PutHandoff(ctx, h)
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
				_ = s.st.PutHandoff(ctx, record)
				s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "recover", true,
					fmt.Sprintf("handoff record rebuilt for %s", sess.HandlerID))
			}
			_ = s.st.DeleteQueueEntry(ctx, sess.ID)
		}
	}

	// 重算坐席容量，修正崩溃造成的计数漂移
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return err
	}
	activeCount := make(map[model.AgentID]int)
	for _, sess := range sessions {
		if sess.State == model.StateAgent && sess.HandlerID != "" {
			activeCount[model.AgentID(sess.HandlerID)]++
		}
	}
	for _, a := range agents {
		a.Active = activeCount[a.ID]
		if a.Active > a.Capacity && a.Capacity > 0 {
			a.Capacity = a.Active
		}
		a.UpdatedAt = now
		if err := s.st.PutAgent(ctx, a); err != nil {
			return err
		}
	}
	// 为在线坐席补投名下会话的积压消息
	for _, sess := range sessions {
		if sess.State != model.StateAgent || sess.HandlerID == "" {
			continue
		}
		if a, err := s.st.GetAgent(ctx, model.AgentID(sess.HandlerID)); err == nil && a.Online {
			_ = s.deliverPendingLocked(ctx, sess)
		}
	}
	return nil
}

// hasCompletedHandoff 判断是否已存在指向 toID 的完成交接单，避免恢复时重复补登
func (s *Service) hasCompletedHandoff(ctx context.Context, sessionID model.SessionID, toID string) bool {
	list, err := s.st.ListHandoffs(ctx, sessionID)
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
