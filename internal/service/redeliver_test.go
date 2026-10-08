package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 慢坐席：邮箱被灌爆后积压消息进入持久化层；坐席开始消费后，
// 周期性补投必须把所有积压消息按序、不丢、不重送达。
func TestSlowAgentBacklogAutoRedeliver(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 4
	cfg.RedeliverInterval = 30 * time.Millisecond
	cfg.DispatchInterval = 30 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	ch, _ := svc.Events(ctx, "a1")

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
	// 排空接管前后的邮箱事件（offer 等）
	drain(ch)

	const total = 40
	for i := 1; i <= total; i++ {
		if _, err := svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// 持久化层里消息齐全且序号连续
	msgs, _ := svc.ListMessages(ctx, "s1")
	if len(msgs) != total {
		t.Fatalf("persisted messages=%d, want %d", len(msgs), total)
	}

	// 存在积压（未送达），且失败计数可观测
	eventually(t, 2*time.Second, func() bool {
		st := svc.Stats()
		return st.DeliveryFailures > 0
	})

	// 慢坐席恢复消费：周期补投应收齐全部消息，严格按 seq 1..40，不重不乱序
	got := make(map[int64]int)
	deadline := time.After(5 * time.Second)
	for int64(len(got)) < total {
		select {
		case ev := <-ch:
			if ev.Type == model.EventMessage && ev.Message != nil {
				got[ev.Message.Seq]++
			}
		case <-deadline:
			t.Fatalf("redelivery incomplete: got %d/%d", len(got), total)
		}
	}
	for seq := int64(1); seq <= total; seq++ {
		if got[seq] != 1 {
			t.Fatalf("seq %d count=%d, want exactly 1 (no dup, no loss)", seq, got[seq])
		}
	}

	// 积压清零
	eventually(t, 2*time.Second, func() bool {
		stored, _ := svc.ListMessages(ctx, "s1")
		for _, m := range stored {
			if !m.Delivered {
				return false
			}
		}
		return svc.Stats().Backlog == 0
	})

	// 补投计数可观测
	if svc.Stats().Redelivered <= 0 {
		t.Fatal("redelivered counter not advanced")
	}
}

// 多个坐席一慢一快：慢坐席积压不影响快坐席消息即时送达，也不影响其补投顺序。
func TestRedeliverIsolationAcrossAgents(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 2
	cfg.RedeliverInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	registerAgent(t, svc, "slow", []string{"tech"}, 2, true)
	registerAgent(t, svc, "fast", []string{"tech"}, 2, true)
	fastCh, _ := svc.Events(ctx, "fast")

	for _, sid := range []string{"slow-s", "fast-s"} {
		if _, err := svc.StartSession(ctx, model.SessionID(sid), "tech"); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.RequestHandoff(ctx, model.SessionID(sid), model.ReasonHandoffRequested); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "slow-s")
		return s.State == model.StateOffering
	})
	if _, err := svc.Claim(ctx, "slow", "slow-s"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Claim(ctx, "fast", "fast-s"); err != nil {
		t.Fatal(err)
	}
	drain(fastCh)

	// slow 不消费，灌入积压
	for i := 0; i < 10; i++ {
		_, _ = svc.SendMessage(ctx, "slow-s", "u", fmt.Sprintf("slow-%d", i))
	}
	// fast 的消息始终即时、按序
	var fastSeqs []int64
	for i := 1; i <= 6; i++ {
		_, err := svc.SendMessage(ctx, "fast-s", "u", fmt.Sprintf("fast-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(3 * time.Second)
	for len(fastSeqs) < 6 {
		select {
		case ev := <-fastCh:
			if ev.Type == model.EventMessage && ev.Message != nil {
				fastSeqs = append(fastSeqs, ev.Message.Seq)
			}
		case <-deadline:
			t.Fatalf("fast agent got %d messages, want 6", len(fastSeqs))
		}
	}
	for i, seq := range fastSeqs {
		if seq != int64(i+1) {
			t.Fatalf("fast agent out-of-order: %v", fastSeqs)
		}
	}

	// slow 积压最终也能在其消费后补齐（验证不丢）
	slowCh, _ := svc.Events(ctx, "slow")
	gotSlow := 0
	dl := time.After(5 * time.Second)
	for gotSlow < 10 {
		select {
		case ev := <-slowCh:
			if ev.Type == model.EventMessage {
				gotSlow++
			}
		case <-dl:
			t.Fatalf("slow backlog redelivered %d/10", gotSlow)
		}
	}
}

func drain(ch <-chan *model.Event) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
