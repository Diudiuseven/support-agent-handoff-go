package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// auditOn 在指定事务视图上写一条审计事件（与业务变更同事务落盘）
func (s *Service) auditOn(ctx context.Context, tx store.Store, sessionID model.SessionID, actorID string, role model.Role, action string, allowed bool, detail string) error {
	ev := audit.Event(sessionID, actorID, role, action, allowed, detail)
	ev.CreatedAt = s.now()
	return tx.AppendAudit(ctx, ev)
}

// auditDenied 在事务回滚后补写一条拒绝审计。
// 被拒绝的操作不产生状态变更，但审计必须可追溯，不能随事务一起回滚。
func (s *Service) auditDenied(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role, action, detail string) {
	ev := audit.Event(sessionID, actorID, role, action, false, detail)
	ev.CreatedAt = s.now()
	_ = s.st.AppendAudit(ctx, ev)
}

// RequestHandoff 机器人请求转人工：bot -> queued，进入排队分发。
// 会话状态、排队条目、交接单与审计在同一事务内整体生效。
func (s *Service) RequestHandoff(ctx context.Context, sessionID model.SessionID, reason model.Reason) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Session
	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if sess.State != model.StateBot {
			return fmt.Errorf("%w: request handoff from state %s", ErrInvalidState, sess.State)
		}
		now := s.now()
		if reason == "" {
			reason = model.ReasonHandoffRequested
		}
		sess.State = model.StateQueued
		sess.HandlerType = ""
		sess.HandlerID = ""
		sess.OfferID = ""
		sess.Reason = reason
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
			FromType:    "bot",
			ToType:      "queue",
			Reason:      reason,
			Status:      model.HandoffCompleted,
			CreatedAt:   now,
			CompletedAt: &now,
		}
		if err := tx.PutHandoff(ctx, h); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, sessionID, "bot", model.RoleBot, "handoff.request", true,
			fmt.Sprintf("bot -> queue, skill=%s", sess.Skill)); err != nil {
			return err
		}
		out = sess
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.kickDispatch()
	return out, nil
}

// Claim 坐席抢占会话：queued/offering -> agent。
// 并发下全局锁串行化：先到先得，未成功者得到 ErrConflict，结果确定。
// 会话状态、坐席容量、队列条目、交接单、审计在同一事务内整体生效。
func (s *Service) Claim(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Handoff
	var sessFinal *model.Session
	var denyDetail string
	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		agent, err := tx.GetAgent(ctx, agentID)
		if err != nil {
			return err
		}

		deny := func(detail string) error {
			denyDetail = detail
			return ErrPermissionDenied
		}

		if agent.Role != model.RoleAgent && agent.Role != model.RoleSupervisor {
			return deny("role not allowed")
		}
		if !agent.Online {
			return deny("agent offline")
		}
		// Active+Reserved 共同计入容量仲裁
		if agent.Active+agent.Reserved >= agent.Capacity {
			return deny("capacity full")
		}
		if agent.Role != model.RoleSupervisor {
			ok := false
			for _, sk := range agent.Skills {
				if sk == sess.Skill {
					ok = true
					break
				}
			}
			if !ok {
				return deny(fmt.Sprintf("skill mismatch: need %s", sess.Skill))
			}
		}
		if sess.State != model.StateQueued && sess.State != model.StateOffering {
			denyDetail = fmt.Sprintf("conflict: session in state %s", sess.State)
			_ = s.auditOn(ctx, tx, sessionID, string(agentID), agent.Role, "offer.claim", false, denyDetail)
			// 冲突审计随回滚丢失，在事务外补写（见下方返回后）
			return ErrConflict
		}

		now := s.now()
		// 原子接管：同一会话同一时刻只有一个处理方
		sess.State = model.StateAgent
		sess.HandlerType = "agent"
		sess.HandlerID = string(agentID)
		sess.OfferID = ""
		sess.Reason = model.ReasonClaim
		sess.UpdatedAt = now
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		agent.Active++
		agent.UpdatedAt = now
		if err := tx.PutAgent(ctx, agent); err != nil {
			return err
		}
		_ = tx.DeleteQueueEntry(ctx, sessionID)

		h := &model.Handoff{
			ID:          newID("handoff"),
			SessionID:   sessionID,
			FromType:    "queue",
			ToType:      "agent",
			ToID:        string(agentID),
			Reason:      model.ReasonClaim,
			Status:      model.HandoffCompleted,
			CreatedAt:   now,
			CompletedAt: &now,
		}
		if err := tx.PutHandoff(ctx, h); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, sessionID, string(agentID), agent.Role, "offer.claim", true,
			fmt.Sprintf("agent %s claimed", agentID)); err != nil {
			return err
		}
		out = h
		sessFinal = sess
		return nil
	})
	if err != nil {
		if denyDetail != "" || err == ErrConflict {
			if a, e2 := s.st.GetAgent(ctx, agentID); e2 == nil {
				s.auditDenied(ctx, sessionID, string(agentID), a.Role, "offer.claim", denyDetail)
			} else {
				s.auditDenied(ctx, sessionID, string(agentID), model.RoleAgent, "offer.claim", denyDetail)
			}
		}
		return nil, err
	}

	// 补投排队期间积压的消息（事务外，非阻塞）
	if err := s.deliverPendingLocked(ctx, sessFinal); err != nil {
		return nil, err
	}
	s.kickDispatch()
	return out, nil
}

// Transfer 坐席发起转接：agent -> transferring，等待目标坐席接受。
//
// 并发容量仲裁：发起时即在目标坐席上做"容量预留"（Reserved++）。
// 在目标接受之前，无论有多少别的会话占用它，Active+Reserved <= Capacity
// 恒成立，因此它接受的那一刻一定真的有位置，不会出现"接受失败、会话卡死"。
// 转接期间处理方仍为原坐席（Active 计数保留，消息继续归原方），
// 接受后原子切换；超时/拒绝/原坐席下线则释放预留并确定性兜底。
func (s *Service) Transfer(ctx context.Context, actorID model.AgentID, role model.Role, sessionID model.SessionID, toAgent model.AgentID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Handoff
	var denyDetail string
	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := tx.GetAgent(ctx, toAgent)
		if err != nil {
			return err
		}

		deny := func(detail string) error {
			denyDetail = detail
			return ErrPermissionDenied
		}

		if role != model.RoleSupervisor && sess.HandlerID != string(actorID) {
			return deny("only the handler or supervisor can transfer")
		}
		if sess.State != model.StateAgent {
			return fmt.Errorf("%w: transfer from state %s", ErrInvalidState, sess.State)
		}
		if !target.Online {
			return deny("target agent offline")
		}
		// 容量仲裁：已处理 + 已预留转接都计入，绝不超卖
		if target.Active+target.Reserved >= target.Capacity {
			return deny("target agent capacity full")
		}
		if target.Role != model.RoleSupervisor {
			ok := false
			for _, sk := range target.Skills {
				if sk == sess.Skill {
					ok = true
					break
				}
			}
			if !ok {
				return deny(fmt.Sprintf("target skill mismatch: need %s", sess.Skill))
			}
		}
		// 不允许向会话原处理坐席自己发起转接
		if sess.HandlerID == string(toAgent) {
			return deny("cannot transfer to current handler")
		}

		now := s.now()
		deadline := now.Add(s.cfg.TransferTimeout)
		sess.State = model.StateTransferring
		sess.Reason = model.ReasonTransferred
		sess.UpdatedAt = now
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		// 预留一个容量位，保证目标接受时必然有位
		target.Reserved++
		target.UpdatedAt = now
		if err := tx.PutAgent(ctx, target); err != nil {
			return err
		}
		h := &model.Handoff{
			ID:        newID("handoff"),
			SessionID: sessionID,
			FromType:  "agent",
			FromID:    sess.HandlerID,
			ToType:    "agent",
			ToID:      string(toAgent),
			Reason:    model.ReasonTransferred,
			Status:    model.HandoffPending,
			CreatedAt: now,
			Deadline:  &deadline,
		}
		if err := tx.PutHandoff(ctx, h); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, sessionID, string(actorID), role, "transfer.request", true,
			fmt.Sprintf("transfer %s -> %s (reserved)", sess.HandlerID, toAgent)); err != nil {
			return err
		}
		out = h
		return nil
	})
	if err != nil {
		if denyDetail != "" {
			s.auditDenied(ctx, sessionID, string(actorID), role, "transfer.request", denyDetail)
		}
		return nil, err
	}

	s.notifyAgent(toAgent, &model.Event{
		Type:      model.EventOffer,
		SessionID: sessionID,
		Text:      fmt.Sprintf("transfer request %s from %s", out.ID, out.FromID),
		At:        s.now(),
	})
	return out, nil
}

// AcceptTransfer 目标坐席接受转接：transferring -> agent，处理方与容量在
// 同一事务内原子切换。容量位在发起转接时已预留，故接受不会因并发占满而失败。
// 唯一的失败情形是目标坐席此刻已下线（无接收方），此时不报错卡死，
// 而是确定性地结束转接：原坐席在线则回到原坐席，否则重新排队。
func (s *Service) AcceptTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var accepted bool
	var fromID string
	var finalSess *model.Session
	var denyDetail string
	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := tx.GetAgent(ctx, agentID)
		if err != nil {
			return err
		}
		if sess.State != model.StateTransferring {
			return fmt.Errorf("%w: accept from state %s", ErrInvalidState, sess.State)
		}

		h, err := pendingHandoff(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if h == nil || (h.ToID != string(agentID) && target.Role != model.RoleSupervisor) {
			denyDetail = "no pending transfer for this agent"
			return ErrPermissionDenied
		}

		now := s.now()
		if !target.Online {
			// 目标已下线：释放预留，转接确定性失败兜底，不留下中间态
			reason := model.ReasonTransferTargetGone
			if err := s.finalizeTransferFailure(ctx, tx, sess, h, reason, now); err != nil {
				return err
			}
			finalSess = sess
			return nil
		}

		fromID = sess.HandlerID
		// 容量切换：原坐席释放 Active；目标消耗预留并占用 Active。
		// 预留保证 target.Active+1 <= target.Capacity，不超卖。
		if fromID != "" && fromID != string(agentID) {
			if from, err := tx.GetAgent(ctx, model.AgentID(fromID)); err == nil {
				if from.Active > 0 {
					from.Active--
				}
				from.UpdatedAt = now
				if err := tx.PutAgent(ctx, from); err != nil {
					return err
				}
			}
		}
		if target.Reserved > 0 {
			target.Reserved--
		}
		target.Active++
		target.UpdatedAt = now
		if err := tx.PutAgent(ctx, target); err != nil {
			return err
		}

		sess.State = model.StateAgent
		sess.HandlerType = "agent"
		sess.HandlerID = string(agentID)
		sess.Reason = model.ReasonTransferred
		sess.UpdatedAt = now
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}

		h.Status = model.HandoffCompleted
		h.CompletedAt = &now
		h.Deadline = nil
		if err := tx.PutHandoff(ctx, h); err != nil {
			return err
		}
		if err := s.auditOn(ctx, tx, sessionID, string(agentID), target.Role, "transfer.accept", true,
			fmt.Sprintf("transfer %s -> %s", fromID, agentID)); err != nil {
			return err
		}
		accepted = true
		finalSess = sess
		return nil
	})
	if err != nil {
		if denyDetail != "" {
			if a, e2 := s.st.GetAgent(ctx, agentID); e2 == nil {
				s.auditDenied(ctx, sessionID, string(agentID), a.Role, "transfer.accept", denyDetail)
			}
		}
		return err
	}

	// 事务提交后再做邮箱通知（非阻塞），不影响一致性
	if accepted {
		if err := s.deliverPendingLocked(ctx, finalSess); err != nil {
			return err
		}
		if fromID != "" && fromID != string(agentID) {
			s.notifyAgent(model.AgentID(fromID), &model.Event{
				Type:      model.EventNotice,
				SessionID: sessionID,
				Text:      fmt.Sprintf("transfer accepted by %s", agentID),
				At:        s.now(),
			})
		}
	} else {
		// 目标下线导致的自动兜底：通知相关方并尝试补投/继续分发
		s.afterRoutingChanged(ctx, finalSess)
	}
	s.kickDispatch()
	return nil
}

// RejectTransfer 目标坐席拒绝转接：释放容量预留；
// 原坐席在线则明确回到原坐席，原坐席已下线则重新排队，原因可区分。
func (s *Service) RejectTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var requeue bool
	var sess *model.Session
	var denyDetail string
	err := s.st.Update(ctx, func(tx store.Store) error {
		var err error
		sess, err = tx.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := tx.GetAgent(ctx, agentID)
		if err != nil {
			return err
		}
		if sess.State != model.StateTransferring {
			return fmt.Errorf("%w: reject from state %s", ErrInvalidState, sess.State)
		}
		h, err := pendingHandoff(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if h == nil || (h.ToID != string(agentID) && target.Role != model.RoleSupervisor) {
			denyDetail = "no pending transfer for this agent"
			return ErrPermissionDenied
		}

		now := s.now()
		// 先释放目标预留，再按原坐席在线与否兜底
		if tgt, err := tx.GetAgent(ctx, model.AgentID(h.ToID)); err == nil {
			if tgt.Reserved > 0 {
				tgt.Reserved--
			}
			tgt.UpdatedAt = now
			if err := tx.PutAgent(ctx, tgt); err != nil {
				return err
			}
		}
		h.Status = model.HandoffFailed
		h.CompletedAt = &now
		h.Deadline = nil
		if err := tx.PutHandoff(ctx, h); err != nil {
			return err
		}

		requeue, err = s.routeBackFromTransfer(ctx, tx, sess, model.ReasonTransferRejected, now)
		if err != nil {
			return err
		}
		return s.auditOn(ctx, tx, sessionID, string(agentID), target.Role, "transfer.reject", true,
			fmt.Sprintf("transfer rejected, requeue=%v", requeue))
	})
	if err != nil {
		if denyDetail != "" {
			if a, e2 := s.st.GetAgent(ctx, agentID); e2 == nil {
				s.auditDenied(ctx, sessionID, string(agentID), a.Role, "transfer.reject", denyDetail)
			}
		}
		return err
	}
	if !requeue {
		s.notifyAgent(model.AgentID(sess.HandlerID), &model.Event{
			Type:      model.EventNotice,
			SessionID: sessionID,
			Text:      fmt.Sprintf("transfer rejected by %s", agentID),
			At:        s.now(),
		})
	}
	s.kickDispatch()
	return nil
}

// finalizeTransferFailure 在事务内确定性地终结一个未完成转接：
// 释放目标坐席预留、把交接单置失败，并把会话路由回原坐席或重新排队。
func (s *Service) finalizeTransferFailure(ctx context.Context, tx store.Store, sess *model.Session, h *model.Handoff, reason model.Reason, now time.Time) error {
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
	h.Status = model.HandoffFailed
	h.CompletedAt = &now
	h.Deadline = nil
	if err := tx.PutHandoff(ctx, h); err != nil {
		return err
	}
	if _, err := s.routeBackFromTransfer(ctx, tx, sess, reason, now); err != nil {
		return err
	}
	return s.auditOn(ctx, tx, sess.ID, "system", model.RoleSystem, "transfer.finalize", true,
		fmt.Sprintf("transfer finalized: reason=%s", reason))
}

// routeBackFromTransfer 转接失败后的确定性路由（事务内）：
// 原坐席仍在线 -> 回到原坐席（其 Active 一直保留，无需调整计数）；
// 原坐席不存在/已下线 -> 释放其 Active，会话重新排队。
// 返回是否重新排队。
func (s *Service) routeBackFromTransfer(ctx context.Context, tx store.Store, sess *model.Session, reason model.Reason, now time.Time) (bool, error) {
	fromID := sess.HandlerID
	from, err := tx.GetAgent(ctx, model.AgentID(fromID))
	online := err == nil && from.Online
	if online {
		sess.State = model.StateAgent
		sess.HandlerType = "agent"
		sess.HandlerID = fromID
		sess.OfferID = ""
		sess.Reason = reason
		sess.UpdatedAt = now
		return false, tx.PutSession(ctx, sess)
	}

	// 原坐席已不可用：释放其占用，重新排队
	if err == nil && from.Active > 0 {
		from.Active--
		from.UpdatedAt = now
		if err := tx.PutAgent(ctx, from); err != nil {
			return true, err
		}
	}
	sess.State = model.StateQueued
	sess.HandlerType = ""
	sess.HandlerID = ""
	sess.OfferID = ""
	sess.Reason = reason
	sess.UpdatedAt = now
	if err := tx.PutSession(ctx, sess); err != nil {
		return true, err
	}
	q := &model.QueueEntry{
		SessionID:     sess.ID,
		Skill:         sess.Skill,
		Priority:      sess.Priority,
		EnqueuedAt:    now,
		QueueDeadline: now.Add(s.cfg.QueueTimeout),
	}
	if err := tx.PutQueueEntry(ctx, q); err != nil {
		return true, err
	}
	rh := &model.Handoff{
		ID:          newID("handoff"),
		SessionID:   sess.ID,
		FromType:    "agent",
		FromID:      fromID,
		ToType:      "queue",
		Reason:      reason,
		Status:      model.HandoffCompleted,
		CreatedAt:   now,
		CompletedAt: &now,
	}
	if err := tx.PutHandoff(ctx, rh); err != nil {
		return true, err
	}
	return true, nil
}

// afterRoutingChanged 路由变化（回到原坐席/重新排队）后的事务外动作：
// 若回到坐席则补投积压消息；重新排队则唤醒分发。
func (s *Service) afterRoutingChanged(ctx context.Context, sess *model.Session) {
	if sess.State == model.StateAgent {
		_ = s.deliverPendingLocked(ctx, sess)
	}
	s.kickDispatch()
}

// pendingHandoff 返回该会话最近一条未完成的交接单（在给定事务视图上读取）
func pendingHandoff(ctx context.Context, st2 store.Store, sessionID model.SessionID) (*model.Handoff, error) {
	list, err := st2.ListHandoffs(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	var latest *model.Handoff
	for _, h := range list {
		if h.Status == model.HandoffPending {
			if latest == nil || h.CreatedAt.After(latest.CreatedAt) {
				latest = h
			}
		}
	}
	return latest, nil
}

// pendingHandoffLocked 返回该会话最近一条未完成的交接单（在主 Store 上读取）
func (s *Service) pendingHandoffLocked(ctx context.Context, sessionID model.SessionID) (*model.Handoff, error) {
	return pendingHandoff(ctx, s.st, sessionID)
}

func (s *Service) ListHandoffs(ctx context.Context, sessionID model.SessionID) ([]*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListHandoffs(ctx, sessionID)
}
