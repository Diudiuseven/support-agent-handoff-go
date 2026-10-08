package service

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// SendMessage 发送一条会话消息：
//   - 序号在全局锁内单调分配（同一会话严格有序，不重不漏）
//   - 按会话当前处理方归属：机器人/坐席；排队中则挂起，接管后补投
//   - 消息落盘与序号推进在同一事务内，崩溃不会出现"有序号无消息"
//   - 坐席邮箱非阻塞投递，满则 Delivered=false 保留，由补投循环周期重投
func (s *Service) SendMessage(ctx context.Context, sessionID model.SessionID, from, body string) (*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var saved *model.Message
	err := s.st.Update(ctx, func(tx store.Store) error {
		sess, err := tx.GetSession(ctx, sessionID)
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
			msg.Delivered = true // 机器人消息无需投坐席邮箱
		default: // queued / offering：处理方未定，挂起待补投
			msg.HandlerType = ""
			msg.HandlerID = ""
		}

		if err := tx.AppendMessage(ctx, msg); err != nil {
			return err
		}
		sess.Seq = seq
		sess.UpdatedAt = msg.CreatedAt
		if err := tx.PutSession(ctx, sess); err != nil {
			return err
		}
		saved = msg
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 邮箱投递在事务外、非阻塞：慢坐席不阻塞主流程；
	// 投递结果回写落盘（FileStore 下同样持久），失败留给周期补投。
	if saved.HandlerType == "agent" && saved.HandlerID != "" {
		s.tryDeliverMessage(ctx, saved)
	}
	return saved, nil
}

// tryDeliverMessage 非阻塞地把一条消息推进坐席邮箱，并持久化投递结果。
// 邮箱满时 Delivered 保持 false 并计入投递失败指标，等待周期补投。
func (s *Service) tryDeliverMessage(ctx context.Context, m *model.Message) {
	agentID := model.AgentID(m.HandlerID)
	cs := s.mailboxStatsFor(agentID)
	cp := *m
	ev := &model.Event{
		Type:      model.EventMessage,
		SessionID: m.SessionID,
		Message:   &cp,
		At:        s.now(),
	}
	select {
	case cs.ch <- ev:
		if !m.Delivered {
			m.Delivered = true
			_ = s.st.UpdateMessage(ctx, m)
		}
	default:
		s.recordDeliveryFailure()
	}
}

// deliverSessionLocked 向会话当前处理坐席按序补投其名下全部未送达消息。
// 返回实际补投条数。邮箱满时停在第一条未投出的消息前（保序），等待下一轮。
// 调用方持有 s.mu。
func (s *Service) deliverSessionLocked(ctx context.Context, sess *model.Session) (int, error) {
	if sess.State != model.StateAgent && sess.State != model.StateTransferring {
		return 0, nil
	}
	if sess.HandlerID == "" {
		return 0, nil
	}
	agent, err := s.st.GetAgent(ctx, model.AgentID(sess.HandlerID))
	if err != nil || !agent.Online {
		return 0, err // 坐席离线，消息保留，等待上线/接管后补投
	}
	msgs, err := s.st.ListMessages(ctx, sess.ID)
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, m := range msgs {
		if m.Delivered {
			continue
		}
		// 只投归属本处理方的消息；排队期挂起消息（HandlerID 为空）
		// 在归属确定时一并认领。
		if m.HandlerID != "" && m.HandlerID != sess.HandlerID {
			continue
		}
		cp := *m
		cp.HandlerType = "agent"
		cp.HandlerID = sess.HandlerID
		cs := s.mailboxStatsFor(model.AgentID(sess.HandlerID))
		select {
		case cs.ch <- &model.Event{
			Type:      model.EventMessage,
			SessionID: sess.ID,
			Message:   &cp,
			At:        s.now(),
		}:
			m.Delivered = true
			m.HandlerType = "agent"
			m.HandlerID = sess.HandlerID
			if err := s.st.UpdateMessage(ctx, m); err != nil {
				return delivered, err
			}
			delivered++
		default:
			// 邮箱满：保序停在此处，下轮周期补投继续，不阻塞其他会话
			s.recordDeliveryFailure()
			return delivered, nil
		}
	}
	return delivered, nil
}

// deliverPendingLocked 会话处理方确定后立即补投一次（兼容既有调用点）
func (s *Service) deliverPendingLocked(ctx context.Context, sess *model.Session) error {
	_, err := s.deliverSessionLocked(ctx, sess)
	return err
}

// redeliverLocked 周期性补投扫描：遍历所有在线坐席名下会话，按序补发
// 积压消息；同时刷新每个邮箱的积压计数用于可观测。返回本轮补投条数。
// 调用方持有 s.mu。
func (s *Service) redeliverLocked(ctx context.Context) (int, error) {
	sessions, err := s.st.ListSessions(ctx)
	if err != nil {
		return 0, err
	}
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return 0, err
	}
	online := make(map[model.AgentID]bool, len(agents))
	for _, a := range agents {
		online[a.ID] = a.Online
	}

	total := 0
	backlog := make(map[model.AgentID]int64)
	for _, sess := range sessions {
		if sess.HandlerID == "" {
			continue
		}
		agentID := model.AgentID(sess.HandlerID)
		if !online[agentID] {
			continue
		}
		msgs, err := s.st.ListMessages(ctx, sess.ID)
		if err != nil {
			return total, err
		}
		for _, m := range msgs {
			if !m.Delivered {
				backlog[agentID]++
			}
		}
		n, err := s.deliverSessionLocked(ctx, sess)
		if err != nil {
			return total, err
		}
		total += n
		if n > 0 {
			backlog[agentID] -= int64(n)
			if backlog[agentID] < 0 {
				backlog[agentID] = 0
			}
		}
	}
	s.mbMu.Lock()
	for id, cs := range s.mailboxes {
		cs.backlog = backlog[id]
	}
	s.mbMu.Unlock()
	return total, nil
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
