package service

import (
	"context"
	"fmt"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

func queueConfig(policy model.Policy) config.Config {
	cfg := testConfig()
	cfg.QueueTimeout = 300 * time.Millisecond
	cfg.OfferTimeout = 150 * time.Millisecond
	cfg.OfferRetries = 1
	cfg.DefaultPolicy = policy
	return cfg
}

// 排队超时：升级策略（升级到高级技能组，原因可区分）
func TestQueueTimeoutEscalate(t *testing.T) {
	svc := newTestService(t, queueConfig(model.PolicyEscalate), nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}

	// 第一次超时：升级
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s1")
		return err == nil && s.Escalated && s.Skill == "senior" && s.Reason == model.ReasonEscalated
	})
	q, _ := svc.ListQueue(ctx)
	if len(q) != 1 || q[0].Skill != "senior" {
		t.Fatalf("queue after escalate: %+v", q)
	}

	// 第二次超时（升级后仍无坐席）：回退机器人
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s1")
		return err == nil && s.State == model.StateBot && s.Reason == model.ReasonQueueTimeout
	})
	q, _ = svc.ListQueue(ctx)
	if len(q) != 0 {
		t.Fatalf("queue should be empty after fallback, got %d", len(q))
	}
	hs, _ := svc.ListHandoffs(ctx, "s1")
	var fallback *model.Handoff
	for _, h := range hs {
		if h.ToType == "bot" {
			fallback = h
		}
	}
	if fallback == nil || fallback.Reason != model.ReasonQueueTimeout {
		t.Fatalf("fallback handoff = %+v", hs)
	}
}

// 排队超时：回退策略
func TestQueueTimeoutFallback(t *testing.T) {
	svc := newTestService(t, queueConfig(model.PolicyFallback), nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s1")
		return err == nil && s.State == model.StateBot && s.Reason == model.ReasonQueueTimeout
	})
}

// 抢占邀请超时：重新排队并再次发起，原因可区分
func TestOfferTimeoutRequeue(t *testing.T) {
	cfg := queueConfig(model.PolicyFallback)
	cfg.OfferTimeout = 80 * time.Millisecond
	cfg.OfferRetries = 3
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}

	// 邀请出现后无人抢占：超时审计存在，且会话被重新邀请（可继续分发）
	eventually(t, 2*time.Second, func() bool {
		audits, err := svc.ListAudit(ctx, "s1")
		if err != nil {
			return false
		}
		for _, e := range audits {
			if e.Action == "offer.timeout" {
				return true
			}
		}
		return false
	})
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s1")
		return err == nil && s.State == model.StateOffering
	})
	// 审计中存在 offer.timeout 记录
	audits, _ := svc.ListAudit(ctx, "s1")
	found := false
	for _, e := range audits {
		if e.Action == "offer.timeout" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing offer.timeout audit")
	}
}

// 慢坐席不得阻塞其他会话：邮箱满时消息不丢、分发不阻塞
func TestSlowAgentDoesNotBlockDispatch(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 2
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})
	if _, err := svc.Claim(ctx, "a1", "s1"); err != nil {
		t.Fatal(err)
	}

	// 向 a1 灌入大量消息且不消费（邮箱容量仅 2），模拟慢坐席
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			_, _ = svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("flood-%d", i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("message sending blocked by slow agent mailbox")
	}

	// 新会话的分发与接管不受影响
	if _, err := svc.StartSession(ctx, "s2", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s2", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s2")
		return s.State == model.StateOffering
	})
	if _, err := svc.Claim(ctx, "a2", "s2"); err != nil {
		t.Fatal(err)
	}
	s2, _ := svc.GetSession(ctx, "s2")
	if s2.HandlerID != "a2" {
		t.Fatalf("s2 handler = %s, want a2", s2.HandlerID)
	}

	// 消息不丢：持久化层序号连续
	msgs, _ := svc.ListMessages(ctx, "s1")
	if len(msgs) != 50 {
		t.Fatalf("s1 messages = %d, want 50", len(msgs))
	}
	for i, m := range msgs {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d: %d", i, m.Seq)
		}
	}
}
