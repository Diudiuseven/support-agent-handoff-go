package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// RequestHandoff 机器人请求转人工：bot -> queued，进入排队分发。
// 会话、排队条目、交接单、审计在同一事务内整体生效。
func (s *Service) RequestHandoff(ctx context.Context, sessionID model.SessionID, reason model.Reason) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Session
	err := s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if sess.State != model.StateBot {
			return fmt.Errorf("%w: request handoff from state %s", ErrInvalidState, sess.State)
		}
		now := s.now()
		sess.State = model.StateQueued
		sess.HandlerType = ""
		sess.HandlerID = ""
		sess.OfferID = ""
		if reason == "" {
			reason = model.ReasonHandoffRequested
		}
		sess.Reason = reason
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
			FromType:    "bot",
			ToType:      "queue",
			Reason:      reason,
			Status:      model.HandoffCompleted,
			CreatedAt:   now,
			CompletedAt: &now,
		}
		if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
			return err
		}
		s.auditLocked(ctx, sessionID, "bot", model.RoleBot, "handoff.request", true,
			fmt.Sprintf("bot -> queue, skill=%s", sess.Skill))
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
// 并发下全局锁串行化 + 单事务：先到先得，未成功者得到 ErrConflict，结果确定；
// 容量在同一临界区"检查并占用"，任何时刻不会超卖。
func (s *Service) Claim(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Handoff
	err := s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		agent, err := s.db(ctx).GetAgent(ctx, agentID)
		if err != nil {
			return err
		}

		deny := func(detail string) error {
			s.auditLocked(ctx, sessionID, string(agentID), agent.Role, "offer.claim", false, detail)
			return ErrPermissionDenied
		}

		if agent.Role != model.RoleAgent && agent.Role != model.RoleSupervisor {
			return deny("role not allowed")
		}
		if !agent.Online {
			return deny("agent offline")
		}
		// 容量：检查与占用在同一事务临界区内，保证接受即真有位置
		if agent.Active >= agent.Capacity {
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
			s.auditLocked(ctx, sessionID, string(agentID), agent.Role, "offer.claim", false,
				fmt.Sprintf("conflict: session in state %s", sess.State))
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
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
			return err
		}
		agent.Active++
		agent.UpdatedAt = now
		if err := s.db(ctx).PutAgent(ctx, agent); err != nil {
			return err
		}
		_ = s.db(ctx).DeleteQueueEntry(ctx, sessionID)

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
		if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
			return err
		}
		s.auditLocked(ctx, sessionID, string(agentID), agent.Role, "offer.claim", true,
			fmt.Sprintf("agent %s claimed", agentID))

		// 补投排队期间积压的消息（同事务标记，提交后投递）
		if err := s.deliverPendingLocked(ctx, sess); err != nil {
			return err
		}
		out = h
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.kickDispatch()
	return out, nil
}

// Transfer 坐席发起转接：agent -> transferring，等待目标坐席接受。
// 转接期间处理方仍为原坐席（消息继续归属原方，计入原坐席容量），
// 不预先占用目标坐席容量；接受时在同一临界区重新仲裁容量。
// 转接单带 Deadline：超时未决由系统自动裁决，会话不会长期停在中间态。
func (s *Service) Transfer(ctx context.Context, actorID model.AgentID, role model.Role, sessionID model.SessionID, toAgent model.AgentID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out *model.Handoff
	err := s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := s.db(ctx).GetAgent(ctx, toAgent)
		if err != nil {
			return err
		}

		deny := func(detail string) error {
			s.auditLocked(ctx, sessionID, string(actorID), role, "transfer.request", false, detail)
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
		// 发起时做一次容量提示性校验（不预留，最终以接受时仲裁为准）
		if target.Active >= target.Capacity {
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

		now := s.now()
		deadline := now.Add(s.cfg.TransferTimeout)
		sess.State = model.StateTransferring
		sess.Reason = model.ReasonTransferred
		sess.UpdatedAt = now
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
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
		if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
			return err
		}
		s.auditLocked(ctx, sessionID, string(actorID), role, "transfer.request", true,
			fmt.Sprintf("transfer %s -> %s, deadline %s", sess.HandlerID, toAgent, deadline.Format(time.RFC3339)))

		s.notifyAgent(toAgent, &model.Event{
			Type:      model.EventOffer,
			SessionID: sessionID,
			Text:      fmt.Sprintf("transfer request %s from %s", h.ID, sess.HandlerID),
			At:        now,
		})
		out = h
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.kickDispatch()
	return out, nil
}

// AcceptTransfer 目标坐席接受转接。容量在同一临界区内最终仲裁：
//   - 目标在线且仍有余量：完成原子切换（原坐席释放、目标占用），交接单完成；
//   - 目标已离线或余量已被其他会话占满：转接不发生，系统自动裁决为
//     "回退原坐席"（原坐席仍在线）或"重新排队"（原坐席已下线），
//     返回 ErrAgentUnavailable 且会话已处于确定的终态，不会卡在 transferring。
func (s *Service) AcceptTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := s.db(ctx).GetAgent(ctx, agentID)
		if err != nil {
			return err
		}
		if sess.State != model.StateTransferring {
			return fmt.Errorf("%w: accept from state %s", ErrInvalidState, sess.State)
		}

		h, err := s.pendingHandoffLocked(ctx, sessionID)
		if err != nil {
			return err
		}
		if h == nil || (h.ToID != string(agentID) && target.Role != model.RoleSupervisor) {
			s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.accept", false,
				"no pending transfer for this agent")
			return ErrPermissionDenied
		}

		// 最终容量仲裁：接受这一刻必须真的还有位置，否则确定性地结束转接
		if !target.Online || target.Active >= target.Capacity {
			s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.accept", false,
				"target unavailable at accept, auto resolving")
			if err := s.resolveFailedTransferLocked(ctx, sess, h, model.ReasonTransferTargetFull); err != nil {
				return err
			}
			// 裁决变更已生效，但本次"接受"对调用方仍是失败，返回明确原因
			return s.commitWithError(fmt.Errorf("%w: capacity occupied or agent offline before accept", ErrAgentUnavailable))
		}

		now := s.now()
		fromID := sess.HandlerID
		sess.State = model.StateAgent
		sess.HandlerType = "agent"
		sess.HandlerID = string(agentID)
		sess.Reason = model.ReasonTransferred
		sess.UpdatedAt = now
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
			return err
		}

		// 容量切换：原坐席释放，目标坐席占用（同一事务，杜绝漂移/超卖）
		if fromID != "" && fromID != string(agentID) {
			if from, ferr := s.db(ctx).GetAgent(ctx, model.AgentID(fromID)); ferr == nil {
				if from.Active > 0 {
					from.Active--
				}
				from.UpdatedAt = now
				_ = s.db(ctx).PutAgent(ctx, from)
			}
		}
		target.Active++
		target.UpdatedAt = now
		if err := s.db(ctx).PutAgent(ctx, target); err != nil {
			return err
		}

		h.Status = model.HandoffCompleted
		h.CompletedAt = &now
		if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
			return err
		}
		s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.accept", true,
			fmt.Sprintf("transfer %s -> %s", fromID, agentID))

		// 补投转接后归属于新坐席的消息（含排队前积压），并通知原坐席
		if err := s.deliverPendingLocked(ctx, sess); err != nil {
			return err
		}
		if fromID != "" && fromID != string(agentID) {
			s.notifyAgent(model.AgentID(fromID), &model.Event{
				Type:      model.EventNotice,
				SessionID: sessionID,
				Text:      fmt.Sprintf("transfer accepted by %s", agentID),
				At:        now,
			})
		}
		return nil
	})
}

// RejectTransfer 目标坐席拒绝转接：回退原坐席；原坐席已下线则重新排队。
// 转接单失败、会话状态、审计在同一事务内整体生效。
func (s *Service) RejectTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		target, err := s.db(ctx).GetAgent(ctx, agentID)
		if err != nil {
			return err
		}
		if sess.State != model.StateTransferring {
			return fmt.Errorf("%w: reject from state %s", ErrInvalidState, sess.State)
		}
		h, err := s.pendingHandoffLocked(ctx, sessionID)
		if err != nil {
			return err
		}
		if h == nil || (h.ToID != string(agentID) && target.Role != model.RoleSupervisor) {
			s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.reject", false,
				"no pending transfer for this agent")
			return ErrPermissionDenied
		}
		s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.reject", true,
			fmt.Sprintf("transfer rejected by %s", agentID))
		return s.resolveFailedTransferLocked(ctx, sess, h, model.ReasonTransferRejected)
	})
}

// resolveFailedTransferLocked 确定性地终结一笔未完成转接（调用方在事务内持锁）：
//   - 原坐席仍在线且仍持有该会话：回退原坐席（StateAgent，容量不变化）；
//   - 原坐席已下线/记录缺失：释放原坐席占用，会话重新进入排队继续分发。
//
// reason 区分来源：transfer_rejected / transfer_timeout / transfer_target_full。
func (s *Service) resolveFailedTransferLocked(ctx context.Context, sess *model.Session, h *model.Handoff, reason model.Reason) error {
	now := s.now()
	h.Status = model.HandoffFailed
	h.CompletedAt = &now
	if err := s.db(ctx).PutHandoff(ctx, h); err != nil {
		return err
	}

	fromID := sess.HandlerID
	from, err := s.db(ctx).GetAgent(ctx, model.AgentID(fromID))
	requeue := fromID == "" || err != nil || !from.Online

	if requeue {
		// 释放原坐席占用（若记录仍在），会话重新排队
		if err == nil && from.Active > 0 {
			from.Active--
			from.UpdatedAt = now
			_ = s.db(ctx).PutAgent(ctx, from)
		}
		sess.State = model.StateQueued
		sess.HandlerType = ""
		sess.HandlerID = ""
		sess.OfferID = ""
		sess.Reason = reason
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
		if err := s.db(ctx).PutHandoff(ctx, rh); err != nil {
			return err
		}
		s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "transfer.resolve", true,
			fmt.Sprintf("requeue, reason=%s", reason))
		s.kickDispatch()
		return nil
	}

	// 回退原坐席：处理方与容量都不变化
	sess.State = model.StateAgent
	sess.HandlerType = "agent"
	sess.HandlerID = fromID
	sess.Reason = reason
	sess.UpdatedAt = now
	if err := s.db(ctx).PutSession(ctx, sess); err != nil {
		return err
	}
	s.auditLocked(ctx, sess.ID, "system", model.RoleSystem, "transfer.resolve", true,
		fmt.Sprintf("back to %s, reason=%s", fromID, reason))
	s.notifyAgent(model.AgentID(fromID), &model.Event{
		Type:      model.EventNotice,
		SessionID: sess.ID,
		Text:      fmt.Sprintf("transfer %s resolved: %s", sess.ID, reason),
		At:        now,
	})
	// 回退原坐席后继续补投（此前若有积压）
	return s.deliverPendingLocked(ctx, sess)
}

// pendingHandoffLocked 返回该会话最近一条未完成的交接单
func (s *Service) pendingHandoffLocked(ctx context.Context, sessionID model.SessionID) (*model.Handoff, error) {
	list, err := s.db(ctx).ListHandoffs(ctx, sessionID)
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

func (s *Service) ListHandoffs(ctx context.Context, sessionID model.SessionID) ([]*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListHandoffs(ctx, sessionID)
}
