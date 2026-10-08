package service

import (
	"context"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// Stats 投递与分发的可观测快照
type Stats struct {
	// Pending 全局未送达（积压）消息总数
	Pending int `json:"pending"`
	// PendingByAgent 每个坐席名下未送达消息数
	PendingByAgent map[string]int `json:"pending_by_agent"`
	// ActiveByAgent 每个坐席当前处理中的会话数
	ActiveByAgent map[string]int `json:"active_by_agent"`
	// CapacityByAgent 每个坐席声明的容量
	CapacityByAgent map[string]int `json:"capacity_by_agent"`
	// RedeliveryAttempts 周期补投累计成功投递的消息数
	RedeliveryAttempts int64 `json:"redelivery_attempts"`
	// DeliveryFailures 因邮箱满等原因未能立即送达的累计次数
	DeliveryFailures int64 `json:"delivery_failures"`
	// Queued 当前排队会话数
	Queued int `json:"queued"`
	// Transferring 当前转接中的会话数
	Transferring int `json:"transferring"`
	// GeneratedAt 快照生成时间
	GeneratedAt time.Time `json:"generated_at"`
}

// Stats 返回当前投递/容量/排队的可观测状态（读取在全局锁内，保证一致）
func (s *Service) Stats(ctx context.Context) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := Stats{
		PendingByAgent:     map[string]int{},
		ActiveByAgent:      map[string]int{},
		CapacityByAgent:    map[string]int{},
		GeneratedAt:        s.now(),
		RedeliveryAttempts: s.redeliveryAttempts,
		DeliveryFailures:   s.deliveryFailures,
	}
	agents, _ := s.st.ListAgents(ctx)
	for _, a := range agents {
		st.ActiveByAgent[string(a.ID)] = a.Active
		st.CapacityByAgent[string(a.ID)] = a.Capacity
	}
	sessions, _ := s.st.ListSessions(ctx)
	for _, sess := range sessions {
		switch sess.State {
		case model.StateTransferring:
			st.Transferring++
		}
		if sess.HandlerID == "" {
			continue
		}
		if sess.State != model.StateAgent && sess.State != model.StateTransferring {
			continue
		}
		msgs, err := s.st.ListMessages(ctx, sess.ID)
		if err != nil {
			continue
		}
		for _, m := range msgs {
			if !m.Delivered {
				st.Pending++
				st.PendingByAgent[sess.HandlerID]++
			}
		}
	}
	queues, _ := s.st.ListQueue(ctx)
	st.Queued = len(queues)
	return st
}
