package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// Recover 重启恢复（调用方为 Run，此时后台循环尚未启动）。
//
// 所有修复按"每个会话一个事务"提交，保证每个修复要么整体生效、要么整体不生效，
// 全程幂等，重复恢复不产生重复处理：
//   - offering：邀请已随进程丢失，回退排队重新分发；
//   - transferring 且转接单 pending：默认作失败处理（释放预留，回退原坐席/排队），
//     重启即确定性收敛，不把跨进程的等待留给客户端；
//   - agent 且交接单 pending：补完成交接单（状态切换与单据落盘之间崩溃）；
//     缺少当前处理方的交接记录则补登，保证可追溯；
//   - queued 但缺排队条目：补齐条目继续推进；
//   - 结束/机器人态残留队列/转接单：清理并对账；
//   - 重算每个坐席的 Active（=名下 agent 态会话数）与 Reserved
//     （=指向该坐席的 pending 转接数），修正一切崩溃漂移；
//   - 在线坐席名下积压消息补投。
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

	var agentSessions []*model.Session
	for _, sess := range sessions {
		var isAgent bool
		err := s.st.Update(ctx, func(tx store.Store) error {
			// 事务内重新读取，保证修复基于同一份一致快照
			cur, err := tx.GetSession(ctx, sess.ID)
			if err != nil {
				return err
			}
			var note string
			switch cur.State {
			case model.StateEnded, model.StateBot:
				_ = tx.DeleteQueueEntry(ctx, cur.ID)
				// 残留 pending 交接单对账失败
				if h, _ := pendingHandoff(ctx, tx, cur.ID); h != nil {
					h.Status = model.HandoffFailed
					h.CompletedAt = &now
					h.Deadline = nil
					if err := tx.PutHandoff(ctx, h); err != nil {
						return err
					}
				}

			case model.StateQueued:
				if _, ok := qBySession[cur.ID]; !ok {
					q := &model.QueueEntry{
						SessionID:     cur.ID,
						Skill:         cur.Skill,
						Priority:      cur.Priority,
						EnqueuedAt:    now,
						QueueDeadline: now.Add(s.cfg.QueueTimeout),
					}
					if err := tx.PutQueueEntry(ctx, q); err != nil {
						return err
					}
					note = "queued entry rebuilt"
				}
				// 排队态不应存在 pending 交接单（崩溃在路由途中）：对账失败
				if h, _ := pendingHandoff(ctx, tx, cur.ID); h != nil {
					if err := releaseReservation(ctx, tx, h.ToID, now); err != nil {
						return err
					}
					h.Status = model.HandoffFailed
					h.CompletedAt = &now
					h.Deadline = nil
					if err := tx.PutHandoff(ctx, h); err != nil {
						return err
					}
					note = "stale pending handoff failed; " + note
				}

			case model.StateOffering:
				cur.State = model.StateQueued
				cur.OfferID = ""
				cur.Reason = model.ReasonRecovered
				cur.UpdatedAt = now
				if err := tx.PutSession(ctx, cur); err != nil {
					return err
				}
				if q, ok := qBySession[cur.ID]; ok {
					q.OfferID = ""
					q.OfferDeadline = time.Time{}
					if err := tx.PutQueueEntry(ctx, q); err != nil {
						return err
					}
				} else {
					q := &model.QueueEntry{
						SessionID:     cur.ID,
						Skill:         cur.Skill,
						Priority:      cur.Priority,
						EnqueuedAt:    now,
						QueueDeadline: now.Add(s.cfg.QueueTimeout),
					}
					if err := tx.PutQueueEntry(ctx, q); err != nil {
						return err
					}
				}
				note = "offering -> queued"

			case model.StateTransferring:
				// 跨进程恢复：未完成转接不延续等待，统一确定性失败并重新排队
				// （重启即重新分发；目标坐席在线时可再次收到邀请）。
				h, _ := pendingHandoff(ctx, tx, cur.ID)
				if h != nil {
					if err := s.requeueFromRecover(ctx, tx, cur, h, now); err != nil {
						return err
					}
					note = "transferring -> queued"
				} else {
					// 无交接单的孤儿转接态：直接重新排队
					cur.State = model.StateQueued
					cur.HandlerType = ""
					cur.HandlerID = ""
					cur.OfferID = ""
					cur.Reason = model.ReasonRecovered
					cur.UpdatedAt = now
					if err := tx.PutSession(ctx, cur); err != nil {
						return err
					}
					if _, ok := qBySession[cur.ID]; !ok {
						q := &model.QueueEntry{
							SessionID:     cur.ID,
							Skill:         cur.Skill,
							Priority:      cur.Priority,
							EnqueuedAt:    now,
							QueueDeadline: now.Add(s.cfg.QueueTimeout),
						}
						_ = tx.PutQueueEntry(ctx, q)
					}
					note = "orphan transferring -> queued"
				}

			case model.StateAgent:
				// 补完成未交接单；若缺少当前处理方的交接记录则补登
				h, _ := pendingHandoff(ctx, tx, cur.ID)
				if h != nil {
					if h.ToID == cur.HandlerID {
						// 接受已把会话切到目标，但交接单尚未落完成：补完成，
						// 预留转占用的计数在下面统一重算
						h.Status = model.HandoffCompleted
						h.CompletedAt = &now
						h.Deadline = nil
						if err := tx.PutHandoff(ctx, h); err != nil {
							return err
						}
						note = fmt.Sprintf("pending handoff completed for %s", cur.HandlerID)
					} else {
						// 会话处理方与 pending 单目标不一致（崩溃在拒绝/回退途中）：
						// 以会话当前归属为准，单据对账失败
						if err := releaseReservation(ctx, tx, h.ToID, now); err != nil {
							return err
						}
						h.Status = model.HandoffFailed
						h.CompletedAt = &now
						h.Deadline = nil
						if err := tx.PutHandoff(ctx, h); err != nil {
							return err
						}
						note = "mismatched pending handoff failed"
					}
				} else if !hasCompletedHandoff(ctx, tx, cur.ID, cur.HandlerID) {
					record := &model.Handoff{
						ID:          newID("handoff"),
						SessionID:   cur.ID,
						FromType:    "queue",
						ToType:      "agent",
						ToID:        cur.HandlerID,
						Reason:      model.ReasonRecovered,
						Status:      model.HandoffCompleted,
						CreatedAt:   cur.UpdatedAt,
						CompletedAt: &now,
					}
					if err := tx.PutHandoff(ctx, record); err != nil {
						return err
					}
					note = fmt.Sprintf("handoff record rebuilt for %s", cur.HandlerID)
				}
				_ = tx.DeleteQueueEntry(ctx, cur.ID)
				isAgent = true
			}
			if note != "" {
				return s.auditOn(ctx, tx, cur.ID, "system", model.RoleSystem, "recover", true, note)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if isAgent {
			agentSessions = append(agentSessions, sess)
		}
	}

	// 重算坐席 Active / Reserved，修正一切崩溃造成的计数漂移。
	// 以会话与 pending 交接单为唯一事实来源，绝不放大声明容量。
	if err := s.st.Update(ctx, func(tx store.Store) error {
		agents, err := tx.ListAgents(ctx)
		if err != nil {
			return err
		}
		finalSessions, err := tx.ListSessions(ctx)
		if err != nil {
			return err
		}
		active := make(map[model.AgentID]int)
		for _, sess := range finalSessions {
			if sess.State == model.StateAgent && sess.HandlerID != "" {
				active[model.AgentID(sess.HandlerID)]++
			}
		}
		reserved := make(map[model.AgentID]int)
		for _, sess := range finalSessions {
			if sess.State != model.StateTransferring {
				continue
			}
			if h, _ := pendingHandoff(ctx, tx, sess.ID); h != nil {
				reserved[model.AgentID(h.ToID)]++
			}
		}
		for _, a := range agents {
			a.Active = active[a.ID]
			a.Reserved = reserved[a.ID]
			a.UpdatedAt = now
			if err := tx.PutAgent(ctx, a); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 为在线坐席补投名下会话的积压消息（恢复后立即一次，之后由周期循环兜底）
	for _, sess := range agentSessions {
		cur, err := s.st.GetSession(ctx, sess.ID)
		if err != nil {
			return err
		}
		if cur.State != model.StateAgent || cur.HandlerID == "" {
			continue
		}
		if a, err := s.st.GetAgent(ctx, model.AgentID(cur.HandlerID)); err == nil && a.Online {
			if _, err := s.deliverSessionLocked(ctx, cur); err != nil {
				return err
			}
		}
	}
	return nil
}

// requeueFromRecover 恢复路径上把一个未完成转接确定性终结并重新排队（事务内）：
// 交接单置失败、释放目标预留、释放原坐席占用、会话回排队、补登 agent->queue 单。
func (s *Service) requeueFromRecover(ctx context.Context, tx store.Store, cur *model.Session, h *model.Handoff, now time.Time) error {
	fromID := cur.HandlerID
	if err := releaseReservation(ctx, tx, h.ToID, now); err != nil {
		return err
	}
	h.Status = model.HandoffFailed
	h.CompletedAt = &now
	h.Deadline = nil
	if err := tx.PutHandoff(ctx, h); err != nil {
		return err
	}
	if fromID != "" {
		if from, err := tx.GetAgent(ctx, model.AgentID(fromID)); err == nil && from.Active > 0 {
			from.Active--
			from.UpdatedAt = now
			if err := tx.PutAgent(ctx, from); err != nil {
				return err
			}
		}
	}
	cur.State = model.StateQueued
	cur.HandlerType = ""
	cur.HandlerID = ""
	cur.OfferID = ""
	cur.Reason = model.ReasonRecovered
	cur.UpdatedAt = now
	if err := tx.PutSession(ctx, cur); err != nil {
		return err
	}
	q := &model.QueueEntry{
		SessionID:     cur.ID,
		Skill:         cur.Skill,
		Priority:      cur.Priority,
		EnqueuedAt:    now,
		QueueDeadline: now.Add(s.cfg.QueueTimeout),
	}
	if err := tx.PutQueueEntry(ctx, q); err != nil {
		return err
	}
	rh := &model.Handoff{
		ID:          newID("handoff"),
		SessionID:   cur.ID,
		FromType:    "agent",
		FromID:      fromID,
		ToType:      "queue",
		Reason:      model.ReasonRecovered,
		Status:      model.HandoffCompleted,
		CreatedAt:   now,
		CompletedAt: &now,
	}
	return tx.PutHandoff(ctx, rh)
}

// releaseReservation 释放目标坐席的一个转接预留（事务内，幂等）
func releaseReservation(ctx context.Context, tx store.Store, agentID string, now time.Time) error {
	if agentID == "" {
		return nil
	}
	a, err := tx.GetAgent(ctx, model.AgentID(agentID))
	if err != nil {
		return nil // 坐席已不存在，无需释放
	}
	if a.Reserved > 0 {
		a.Reserved--
		a.UpdatedAt = now
		return tx.PutAgent(ctx, a)
	}
	return nil
}

// hasCompletedHandoff 判断是否已存在指向 toID 的完成交接单，避免恢复时重复补登
func hasCompletedHandoff(ctx context.Context, st2 store.Store, sessionID model.SessionID, toID string) bool {
	list, err := st2.ListHandoffs(ctx, sessionID)
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
