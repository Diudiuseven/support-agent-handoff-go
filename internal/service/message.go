package service

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// SendMessage 发送一条会话消息：
//   - 序号在全局锁内单调分配（同一会话严格有序，不重不漏）
//   - 按会话当前处理方归属：机器人/坐席；排队中则挂起，接管后补投
//   - 交接与消息在同一临界区排序：消息要么在交接前归旧方，要么在交接后归新方
func (s *Service) SendMessage(ctx context.Context, sessionID model.SessionID, from, body string) (*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if sess.State == model.StateEnded {
		return nil, ErrInvalidState
	}

	seq := sess.Seq + 1
	msg := &model.Message{
		ID:        fmt.Sprintf("%s-%d", sessionID, seq),
		SessionID: sessionID,
		Seq:       seq,
		From:      from,
		Body:      body,
		CreatedAt: s.now(),
	}
	switch sess.State {
	case model.StateAgent, model.StateTransferring:
		msg.HandlerType = "agent"
		msg.HandlerID = sess.HandlerID
	case model.StateBot:
		msg.HandlerType = "bot"
	default: // queued / offering：处理方未定，挂起待补投
		msg.HandlerType = ""
		msg.HandlerID = ""
	}

	if err := s.st.AppendMessage(ctx, msg); err != nil {
		return nil, err
	}
	sess.Seq = seq
	sess.UpdatedAt = msg.CreatedAt
	if err := s.st.PutSession(ctx, sess); err != nil {
		return nil, err
	}

	// 投递：坐席邮箱非阻塞，满则保留 Delivered=false，由接管/恢复时补投
	if msg.HandlerType == "agent" && msg.HandlerID != "" {
		ev := &model.Event{
			Type:      model.EventMessage,
			SessionID: sessionID,
			Message:   msg,
			At:        msg.CreatedAt,
		}
		select {
		case s.mailboxFor(model.AgentID(msg.HandlerID)) <- ev:
			msg.Delivered = true
			_ = s.st.UpdateMessage(ctx, msg)
		default:
			msg.Delivered = false
		}
	} else {
		msg.Delivered = true // 机器人处理或挂起（挂起消息在接管时补投）
		if msg.HandlerType == "" {
			msg.Delivered = false
		}
		_ = s.st.UpdateMessage(ctx, msg)
	}
	return msg, nil
}

// deliverPendingLocked 会话处理方确定后，补投所有未投递消息（调用方持有 s.mu）
func (s *Service) deliverPendingLocked(ctx context.Context, sess *model.Session) error {
	if sess.State != model.StateAgent || sess.HandlerID == "" {
		return nil
	}
	msgs, err := s.st.ListMessages(ctx, sess.ID)
	if err != nil {
		return err
	}
	ch := s.mailboxFor(model.AgentID(sess.HandlerID))
	undelivered := make([]*model.Message, 0)
	for _, m := range msgs {
		if m.Delivered {
			continue
		}
		undelivered = append(undelivered, m)
	}
	for _, m := range undelivered {
		cp := *m
		cp.HandlerType = "agent"
		cp.HandlerID = sess.HandlerID
		ev := &model.Event{
			Type:      model.EventMessage,
			SessionID: sess.ID,
			Message:   &cp,
			At:        s.now(),
		}
		select {
		case ch <- ev:
			m.Delivered = true
			m.HandlerType = "agent"
			m.HandlerID = sess.HandlerID
			if err := s.st.UpdateMessage(ctx, m); err != nil {
				return err
			}
		default:
			// 邮箱满：跳过，等下次投递机会，不阻塞主流程
			return nil
		}
	}
	return nil
}

func (s *Service) ListMessages(ctx context.Context, sessionID model.SessionID) ([]*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListMessages(ctx, sessionID)
}

func (s *Service) ListAudit(ctx context.Context, sessionID model.SessionID) ([]*model.AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ListAudit(ctx, sessionID)
}
