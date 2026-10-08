package service

import (
	"context"
	"sort"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// redeliverLocked 周期性补投：扫描每个在线坐席名下处理中/转接中的会话，
// 将持久化层中 Delivered=false 的消息按 Seq 顺序补投到坐席邮箱。
//
// 规则：
//   - 只补投给当前在线的处理坐席（不在线的等上线/接管/恢复时补）；
//   - 严格按 Seq 升序，遇到邮箱满立即停止该会话的补投（保证不乱序、不跳号），
//     下一个分发周期继续；慢坐席不会阻塞其他坐席/会话；
//   - 只有成功进入邮箱才把 Delivered 置 true 并随事务落盘，因此正常运行期
//     不重复、不丢失；全部尝试在 s.mu 下串行，任何时刻不会并发补投同一条。
func (s *Service) redeliverLocked(ctx context.Context) error {
	sessions, err := s.db(ctx).ListSessions(ctx)
	if err != nil {
		return err
	}
	// 会话按 ID 排序，保证补投扫描顺序稳定、测试可重复
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].ID < sessions[j].ID })
	for _, sess := range sessions {
		if sess.HandlerID == "" {
			continue
		}
		if sess.State != model.StateAgent && sess.State != model.StateTransferring {
			continue
		}
		a, err := s.db(ctx).GetAgent(ctx, model.AgentID(sess.HandlerID))
		if err != nil {
			continue // 坐席记录缺失等异常：跳过，后续恢复流程裁决
		}
		if !a.Online {
			continue
		}
		if err := s.deliverPendingLocked(ctx, sess); err != nil {
			return err
		}
	}
	return nil
}

// pendingCountLocked 统计某坐席名下当前积压（未送达）的消息数（用于观测）
func (s *Service) pendingCountLocked(ctx context.Context, agentID model.AgentID) int {
	n := 0
	sessions, err := s.db(ctx).ListSessions(ctx)
	if err != nil {
		return 0
	}
	for _, sess := range sessions {
		if sess.State != model.StateAgent && sess.State != model.StateTransferring {
			continue
		}
		if sess.HandlerID != string(agentID) {
			continue
		}
		msgs, err := s.db(ctx).ListMessages(ctx, sess.ID)
		if err != nil {
			continue
		}
		for _, m := range msgs {
			if !m.Delivered {
				n++
			}
		}
	}
	return n
}
