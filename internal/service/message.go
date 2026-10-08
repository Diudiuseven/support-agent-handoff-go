package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// SendMessage 发送一条会话消息：
//   - 序号在全局锁内单调分配（同一会话严格有序，不重不漏）
//   - 按会话当前处理方归属：机器人/坐席；排队中则挂起，接管后补投
//   - 交接与消息在同一临界区排序：消息要么在交接前归旧方，要么在交接后归新方
func (s *Service) SendMessage(ctx context.Context, sessionID model.SessionID, from, body string) (*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var saved *model.Message
	err := s.inTx(ctx, func() error {
		sess, err := s.db(ctx).GetSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if sess.State == model.StateEnded {
			return ErrInvalidState
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

		if err := s.db(ctx).AppendMessage(ctx, msg); err != nil {
			return err
		}
		sess.Seq = seq
		sess.UpdatedAt = msg.CreatedAt
		if err := s.db(ctx).PutSession(ctx, sess); err != nil {
			return err
		}

		// 机器人处理视为已送达；排队/邀请期间消息挂起（Delivered=false）
		if msg.HandlerType == "bot" {
			msg.Delivered = true
			if err := s.db(ctx).UpdateMessage(ctx, msg); err != nil {
				return err
			}
			saved = msg
			return nil
		}
		if msg.HandlerType == "agent" && msg.HandlerID != "" {
			// 事务内立即占用邮箱槽位（持 s.mu，顺序与落盘一致）；
			// 满则 Delivered 保持 false，由周期补投循环送达。
			agentID := model.AgentID(msg.HandlerID)
			ev := &model.Event{
				Type:      model.EventMessage,
				SessionID: sessionID,
				Message:   msg,
				At:        msg.CreatedAt,
			}
			msg.Delivered = s.deliverMessageLocked(agentID, ev)
			if err := s.db(ctx).UpdateMessage(ctx, msg); err != nil {
				return err
			}
		}
		saved = msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return saved, nil
}

// mailboxHasRoom 非阻塞探测邮箱当前是否可立即写入（不真正写入）。
func (s *Service) mailboxHasRoom(agentID model.AgentID) bool {
	ch := s.mailboxFor(agentID)
	return len(ch) < cap(ch)
}

// deliverPendingLocked 把会话当前处理方名下所有 Delivered=false 的消息按 Seq
// 顺序补投到其邮箱（调用方持有 s.mu 且处于事务内）。
// 邮箱满时立即停止（不跳过、不跳号），由下一周期/下一次状态变更继续，
// 因此最终送达且严格有序；慢坐席不会阻塞其他会话分发。
func (s *Service) deliverPendingLocked(ctx context.Context, sess *model.Session) error {
	if (sess.State != model.StateAgent && sess.State != model.StateTransferring) || sess.HandlerID == "" {
		return nil
	}
	msgs, err := s.db(ctx).ListMessages(ctx, sess.ID)
	if err != nil {
		return err
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
	agentID := model.AgentID(sess.HandlerID)
	for _, m := range msgs {
		if m.Delivered {
			continue
		}
		// 严格按 Seq：只在邮箱确实有空位时投递；一旦满立即停止该会话，
		// 不跳过、不跳号，因此最终送达且会话内严格有序。
		if !s.mailboxHasRoom(agentID) {
			s.deliveryFailures++
			return nil
		}
		cp := *m
		cp.HandlerType = "agent"
		cp.HandlerID = sess.HandlerID
		ev := &model.Event{
			Type:      model.EventMessage,
			SessionID: sess.ID,
			Message:   &cp,
			At:        s.now(),
		}
		ok := s.deliverMessageLocked(agentID, ev)
		s.redeliveryAttempts++
		m.HandlerType = "agent"
		m.HandlerID = sess.HandlerID
		m.Delivered = ok
		if err := s.db(ctx).UpdateMessage(ctx, m); err != nil {
			return err
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
