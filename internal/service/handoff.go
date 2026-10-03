package service

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// RequestHandoff 机器人请求转人工：bot -> queued，进入排队分发
func (s *Service) RequestHandoff(ctx context.Context, sessionID model.SessionID, reason model.Reason) (*model.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if sess.State != model.StateBot {
		return nil, fmt.Errorf("%w: request handoff from state %s", ErrInvalidState, sess.State)
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
	if err := s.st.PutSession(ctx, sess); err != nil {
		return nil, err
	}
	q := &model.QueueEntry{
		SessionID:     sess.ID,
		Skill:         sess.Skill,
		Priority:      sess.Priority,
		EnqueuedAt:    now,
		QueueDeadline: now.Add(s.cfg.QueueTimeout),
	}
	if err := s.st.PutQueueEntry(ctx, q); err != nil {
		return nil, err
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
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return nil, err
	}
	s.auditLocked(ctx, sessionID, "bot", model.RoleBot, "handoff.request", true,
		fmt.Sprintf("bot -> queue, skill=%s", sess.Skill))
	s.kickDispatch()
	return sess, nil
}

// Claim 坐席抢占会话：queued/offering -> agent。
// 并发下全局锁串行化：先到先得，未成功者得到 ErrConflict，结果确定。
func (s *Service) Claim(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	agent, err := s.st.GetAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}

	deny := func(detail string) (*model.Handoff, error) {
		s.auditLocked(ctx, sessionID, string(agentID), agent.Role, "offer.claim", false, detail)
		return nil, ErrPermissionDenied
	}

	if agent.Role != model.RoleAgent && agent.Role != model.RoleSupervisor {
		return deny("role not allowed")
	}
	if !agent.Online {
		return deny("agent offline")
	}
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
		return nil, ErrConflict
	}

	now := s.now()
	// 原子接管：同一会话同一时刻只有一个处理方
	sess.State = model.StateAgent
	sess.HandlerType = "agent"
	sess.HandlerID = string(agentID)
	sess.OfferID = ""
	sess.Reason = model.ReasonClaim
	sess.UpdatedAt = now
	if err := s.st.PutSession(ctx, sess); err != nil {
		return nil, err
	}
	agent.Active++
	agent.UpdatedAt = now
	if err := s.st.PutAgent(ctx, agent); err != nil {
		return nil, err
	}
	_ = s.st.DeleteQueueEntry(ctx, sessionID)

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
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return nil, err
	}
	s.auditLocked(ctx, sessionID, string(agentID), agent.Role, "offer.claim", true,
		fmt.Sprintf("agent %s claimed", agentID))

	// 补投排队期间积压的消息
	if err := s.deliverPendingLocked(ctx, sess); err != nil {
		return nil, err
	}
	s.kickDispatch()
	return h, nil
}

// Transfer 坐席发起转接：agent -> transferring，等待目标坐席接受。
// 转接期间处理方仍为原坐席（消息继续归属原方），接受后原子切换。
func (s *Service) Transfer(ctx context.Context, actorID model.AgentID, role model.Role, sessionID model.SessionID, toAgent model.AgentID) (*model.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	actor, err := s.st.GetAgent(ctx, actorID)
	if err != nil {
		return nil, err
	}
	target, err := s.st.GetAgent(ctx, toAgent)
	if err != nil {
		return nil, err
	}

	deny := func(detail string) (*model.Handoff, error) {
		s.auditLocked(ctx, sessionID, string(actorID), role, "transfer.request", false, detail)
		return nil, ErrPermissionDenied
	}

	if role != model.RoleSupervisor && sess.HandlerID != string(actorID) {
		return deny("only the handler or supervisor can transfer")
	}
	if sess.State != model.StateAgent {
		return nil, fmt.Errorf("%w: transfer from state %s", ErrInvalidState, sess.State)
	}
	if !target.Online {
		return deny("target agent offline")
	}
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
	sess.State = model.StateTransferring
	sess.Reason = model.ReasonTransferred
	sess.UpdatedAt = now
	if err := s.st.PutSession(ctx, sess); err != nil {
		return nil, err
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
	}
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return nil, err
	}
	s.auditLocked(ctx, sessionID, string(actorID), role, "transfer.request", true,
		fmt.Sprintf("transfer %s -> %s", sess.HandlerID, toAgent))

	s.notifyAgent(toAgent, &model.Event{
		Type:      model.EventOffer,
		SessionID: sessionID,
		Text:      fmt.Sprintf("transfer request %s from %s", h.ID, sess.HandlerID),
		At:        now,
	})
	_ = actor
	return h, nil
}

// AcceptTransfer 目标坐席接受转接：transferring -> agent，处理方原子切换
func (s *Service) AcceptTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	target, err := s.st.GetAgent(ctx, agentID)
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
	if !target.Online || target.Active >= target.Capacity {
		s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.accept", false,
			"target unavailable")
		return ErrAgentUnavailable
	}

	now := s.now()
	fromID := sess.HandlerID
	sess.State = model.StateAgent
	sess.HandlerType = "agent"
	sess.HandlerID = string(agentID)
	sess.Reason = model.ReasonTransferred
	sess.UpdatedAt = now
	if err := s.st.PutSession(ctx, sess); err != nil {
		return err
	}

	// 容量切换：原坐席释放，目标坐席占用
	if fromID != "" {
		if from, err := s.st.GetAgent(ctx, model.AgentID(fromID)); err == nil {
			if from.Active > 0 {
				from.Active--
			}
			from.UpdatedAt = now
			_ = s.st.PutAgent(ctx, from)
		}
	}
	target.Active++
	target.UpdatedAt = now
	if err := s.st.PutAgent(ctx, target); err != nil {
		return err
	}

	h.Status = model.HandoffCompleted
	h.CompletedAt = &now
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return err
	}
	s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.accept", true,
		fmt.Sprintf("transfer %s -> %s", fromID, agentID))

	// 补投转接期间积压的消息，并通知原坐席
	if err := s.deliverPendingLocked(ctx, sess); err != nil {
		return err
	}
	if fromID != "" {
		s.notifyAgent(model.AgentID(fromID), &model.Event{
			Type:      model.EventNotice,
			SessionID: sessionID,
			Text:      fmt.Sprintf("transfer accepted by %s", agentID),
			At:        now,
		})
	}
	s.kickDispatch()
	return nil
}

// RejectTransfer 目标坐席拒绝转接：回退原坐席；原坐席已下线则重新排队
func (s *Service) RejectTransfer(ctx context.Context, agentID model.AgentID, sessionID model.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	target, err := s.st.GetAgent(ctx, agentID)
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
	if h == nil || h.ToID != string(agentID) {
		s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.reject", false,
			"no pending transfer for this agent")
		return ErrPermissionDenied
	}

	now := s.now()
	h.Status = model.HandoffFailed
	h.CompletedAt = &now
	if err := s.st.PutHandoff(ctx, h); err != nil {
		return err
	}

	from, err := s.st.GetAgent(ctx, model.AgentID(sess.HandlerID))
	requeue := err != nil || !from.Online
	if requeue {
		sess.State = model.StateQueued
		sess.HandlerType = ""
		sess.HandlerID = ""
		sess.Reason = model.ReasonTransferRejected
		q := &model.QueueEntry{
			SessionID:     sess.ID,
			Skill:         sess.Skill,
			Priority:      sess.Priority,
			EnqueuedAt:    now,
			QueueDeadline: now.Add(s.cfg.QueueTimeout),
		}
		_ = s.st.PutQueueEntry(ctx, q)
	} else {
		sess.State = model.StateAgent
		sess.Reason = model.ReasonTransferRejected
	}
	sess.UpdatedAt = now
	if err := s.st.PutSession(ctx, sess); err != nil {
		return err
	}
	s.auditLocked(ctx, sessionID, string(agentID), target.Role, "transfer.reject", true,
		fmt.Sprintf("transfer rejected, requeue=%v", requeue))
	if !requeue {
		s.notifyAgent(model.AgentID(sess.HandlerID), &model.Event{
			Type:      model.EventNotice,
			SessionID: sessionID,
			Text:      fmt.Sprintf("transfer rejected by %s", agentID),
			At:        now,
		})
	}
	s.kickDispatch()
	return nil
}

// pendingHandoffLocked 返回该会话最近一条未完成的交接单
func (s *Service) pendingHandoffLocked(ctx context.Context, sessionID model.SessionID) (*model.Handoff, error) {
	list, err := s.st.ListHandoffs(ctx, sessionID)
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
