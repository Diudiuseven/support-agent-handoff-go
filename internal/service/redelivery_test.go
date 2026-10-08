package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 慢坐席：邮箱被灌满后，积压消息由周期补投循环稳定送达；
// 坐席恢复消费后，所有积压消息按 Seq 顺序最终送达，不丢、不重、不乱序。
func TestSlowAgentPeriodicRedelivery(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 4
	cfg.DispatchInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	ch, _ := svc.Events(ctx, "a1")

	// 先把邮箱里的 offer 等事件排空，只关注消息
	drain(ch)

	const total = 40
	// 快速灌入 40 条消息但不消费，邮箱很快打满，其余积压在持久化层
	for i := 1; i <= total; i++ {
		if _, err := svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// 持久化层：序号 1..40 连续，部分积压未送达
	msgs, _ := svc.ListMessages(ctx, "s1")
	if len(msgs) != total {
		t.Fatalf("persisted messages = %d, want %d", len(msgs), total)
	}
	undelivered := 0
	for _, m := range msgs {
		if !m.Delivered {
			undelivered++
		}
	}
	if undelivered == 0 {
		t.Fatal("expected backlog, all messages marked delivered")
	}

	// 慢坐席开始以稳定速率消费：最终应收齐 1..40，顺序严格递增，无重复
	got := map[int64]bool{}
	var lastSeq int64
	deadline := time.After(5 * time.Second)
	for len(got) < total {
		select {
		case ev := <-ch:
			if ev.Type != model.EventMessage || ev.Message == nil {
				continue
			}
			seq := ev.Message.Seq
			if got[seq] {
				t.Fatalf("duplicate delivery of seq %d", seq)
			}
			if seq <= lastSeq {
				t.Fatalf("out of order: seq %d after %d", seq, lastSeq)
			}
			lastSeq = seq
			got[seq] = true
		case <-deadline:
			t.Fatalf("only received %d/%d messages: %v", len(got), total, got)
		}
	}

	// 全部消息最终 Delivered=true
	msgs, _ = svc.ListMessages(ctx, "s1")
	for i, m := range msgs {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq gap: index %d seq %d", i, m.Seq)
		}
		if !m.Delivered {
			t.Fatalf("seq %d still not delivered", m.Seq)
		}
	}
}

// 积压期间别的会话/坐席的分发与接管不受慢坐席影响，
// 且其他坐席的消息即时投递、顺序正确。
func TestRedeliveryDoesNotBlockOthers(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 2
	cfg.DispatchInterval = 15 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"}) // 慢坐席
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
	ch1, _ := svc.Events(ctx, "a1")
	drain(ch1)

	// 灌满 a1
	for i := 0; i < 30; i++ {
		_, _ = svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("slow-%d", i))
	}

	// a2 正常接管新会话并即时收到消息
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
	ch2, _ := svc.Events(ctx, "a2")
	if _, err := svc.Claim(ctx, "a2", "s2"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := svc.SendMessage(ctx, "s2", "u", fmt.Sprintf("fast-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// a2 应很快收齐 5 条且有序
	got := map[int64]bool{}
	var last int64
	deadline := time.After(2 * time.Second)
	for len(got) < 5 {
		select {
		case ev := <-ch2:
			if ev.Type != model.EventMessage || ev.Message == nil || ev.SessionID != "s2" {
				continue
			}
			if ev.Message.Seq <= last {
				t.Fatalf("a2 out of order: %d after %d", ev.Message.Seq, last)
			}
			last = ev.Message.Seq
			got[ev.Message.Seq] = true
		case <-deadline:
			t.Fatalf("a2 only got %d/5", len(got))
		}
	}

	// 可观测：存在失败计数与积压
	st := svc.Stats(ctx)
	if st.PendingByAgent["a1"] <= 0 {
		t.Fatalf("expected backlog for a1, stats=%+v", st)
	}
	if st.DeliveryFailures == 0 {
		t.Fatalf("expected delivery failure counter > 0, stats=%+v", st)
	}
}

// 坐席恢复上线后，积压消息继续补投（不依赖恰好发生别的会话操作）
func TestRedeliveryAfterAgentReconnect(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 4
	cfg.DispatchInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	// 直接让 a1 接管，随后其邮箱无消费者（模拟断开）；先记录旧 channel
	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	if err := svc.AgentOffline(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	// 下线期间消息不补投（会话已重排队）；重新上线一个新坐席接管
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})
	ch2, _ := svc.Events(ctx, "a2")
	if _, err := svc.Claim(ctx, "a2", "s1"); err != nil {
		t.Fatal(err)
	}
	drain(ch2)
	// 灌入积压（a2 暂不消费）
	for i := 0; i < 20; i++ {
		_, _ = svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("m%d", i))
	}
	// 现在 a2 开始消费：周期补投应收齐
	got := map[int64]bool{}
	deadline := time.After(5 * time.Second)
	for len(got) < 20 {
		select {
		case ev := <-ch2:
			if ev.Type == model.EventMessage && ev.Message != nil {
				got[ev.Message.Seq] = true
			}
		case <-deadline:
			t.Fatalf("a2 received %d/20 after reconnect", len(got))
		}
	}
}

// 同一坐席多个会话的积压：每个会话内部按 Seq 有序，跨会话不串号、不重复。
func TestRedeliveryMultiSessionOrder(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 2
	cfg.DispatchInterval = 15 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	registerAgent(t, svc, "a1", []string{"tech"}, 5, true)
	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	setupHandledSession(t, svc, "s2", "a1", []string{"tech"})
	ch, _ := svc.Events(ctx, "a1")
	drain(ch)

	// 全部消息在无消费者的情况下灌满：形成两个会话各自的积压，
	// 之后由周期补投按每会话 Seq 顺序送达。
	for i := 0; i < 15; i++ {
		_, _ = svc.SendMessage(ctx, "s1", "u", fmt.Sprintf("a-%d", i))
		_, _ = svc.SendMessage(ctx, "s2", "u", fmt.Sprintf("b-%d", i))
	}
	seen := map[model.SessionID]map[int64]bool{
		"s1": {}, "s2": {},
	}
	last := map[model.SessionID]int64{"s1": 0, "s2": 0}
	deadline := time.After(5 * time.Second)
	for len(seen["s1"]) < 15 || len(seen["s2"]) < 15 {
		select {
		case ev := <-ch:
			if ev.Type != model.EventMessage || ev.Message == nil {
				continue
			}
			sid := ev.SessionID
			seq := ev.Message.Seq
			if seen[sid][seq] {
				t.Fatalf("dup %s seq %d", sid, seq)
			}
			// 每个会话内部必须严格递增；跨会话（共享邮箱）允许交错
			if seq <= last[sid] {
				t.Fatalf("order %s: %d after %d", sid, seq, last[sid])
			}
			last[sid] = seq
			seen[sid][seq] = true
		case <-deadline:
			t.Fatalf("got s1=%d s2=%d", len(seen["s1"]), len(seen["s2"]))
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

var _ = config.DefaultConfig
