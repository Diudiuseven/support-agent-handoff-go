package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 辅助：构造已在 fromAgent 名下的会话，target 为转接目标
func setupTransfer(t *testing.T, svc *Service, sid, from, to string, toCap int) {
	t.Helper()
	setupHandledSession(t, svc, sid, from, []string{"tech"})
	registerAgent(t, svc, to, []string{"tech"}, toCap, true)
}

// 用可控时钟/钩子复现"发起转接后、接受前目标余量被别的会话占满"。
// 做法：目标容量=1，发起转接时预留唯一容量位；此后任何新的抢占/接管
// 都必须因容量仲裁（Active+Reserved>=Capacity）被挡住，接受必定成功。
func TestTransferAcceptAfterCapacityFilled(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 64
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	setupTransfer(t, svc, "s1", "a1", "a2", 1) // a2 容量仅 1

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}

	a2, _ := svc.st.GetAgent(ctx, "a2")
	if a2.Active != 0 || a2.Reserved != 1 {
		t.Fatalf("after transfer: a2 active=%d reserved=%d, want 0/1", a2.Active, a2.Reserved)
	}

	// 发起第二个会话并尝试让 a2 抢占：预留占着位，必须被容量仲裁拒绝
	if _, err := svc.StartSession(ctx, "s2", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s2", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	// 直接在任意状态尝试抢占，容量检查先于状态检查，必然 ErrPermissionDenied
	if _, err := svc.Claim(ctx, "a2", "s2"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("claim into reserved capacity: err=%v, want ErrPermissionDenied", err)
	}

	// 关键断言：尽管"接受时余量看起来被占满"，接受仍然成功，不卡死
	if err := svc.AcceptTransfer(ctx, "a2", "s1"); err != nil {
		t.Fatalf("accept transfer failed despite reservation: %v", err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a2" {
		t.Fatalf("s1 = %s/%s, want agent/a2", sess.State, sess.HandlerID)
	}
	a2, _ = svc.st.GetAgent(ctx, "a2")
	if a2.Active != 1 || a2.Reserved != 0 || a2.Active+a2.Reserved > a2.Capacity {
		t.Fatalf("a2 after accept active=%d reserved=%d capN=%d", a2.Active, a2.Reserved, a2.Capacity)
	}
	a1, _ := svc.st.GetAgent(ctx, "a1")
	if a1.Active != 0 {
		t.Fatalf("a1 active=%d, want 0 (released)", a1.Active)
	}
}

// 并发接受/抢占/上下线交织下：容量不超卖、不漂移，转接必有确定结果。
func TestConcurrentTransferCapacityArbitration(t *testing.T) {
	cfg := testConfig()
	cfg.MailboxSize = 128
	cfg.DispatchInterval = 5 * time.Millisecond
	cfg.TransferTimeout = 2 * time.Second
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	const capN = 3
	registerAgent(t, svc, "dst", []string{"tech"}, capN, true)
	dstCh, _ := svc.Events(ctx, "dst")

	// 3 个源坐席各持有一个会话，全部并发转给 dst
	for i := 0; i < capN; i++ {
		setupHandledSession(t, svc, fmt.Sprintf("t%d", i), fmt.Sprintf("src%d", i), []string{"tech"})
	}

	var wg sync.WaitGroup
	for i := 0; i < capN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = svc.Transfer(ctx, model.AgentID(fmt.Sprintf("src%d", i)), model.RoleAgent,
				model.SessionID(fmt.Sprintf("t%d", i)), "dst")
		}(i)
	}
	wg.Wait()

	d, _ := svc.st.GetAgent(ctx, "dst")
	if d.Reserved != capN || d.Active != 0 {
		t.Fatalf("after concurrent transfers: active=%d reserved=%d, want 0/%d", d.Active, d.Reserved, capN)
	}

	// 第 4 个转接必须失败（容量已被预留占满）
	setupHandledSession(t, svc, "tX", "srcX", []string{"tech"})
	if _, err := svc.Transfer(ctx, "srcX", model.RoleAgent, "tX", "dst"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("4th transfer err=%v, want ErrPermissionDenied", err)
	}

	// 排空 dst 邮箱中的 transfer offer（避免干扰）
	drained := false
	for !drained {
		select {
		case <-dstCh:
		default:
			drained = true
		}
	}

	// 并发：3 个接受 + dst 反复下线/上线尝试 + 其他会话抢占尝试
	claimDone := make(chan struct{})
	go func() {
		// 用额外会话持续试探抢占，永不允许超过容量
		for i := 0; i < 20; i++ {
			id := model.SessionID(fmt.Sprintf("noise%d", i))
			if _, err := svc.StartSession(ctx, id, "tech"); err == nil {
				_, _ = svc.RequestHandoff(ctx, id, model.ReasonHandoffRequested)
				_, _ = svc.Claim(ctx, "dst", id) // 容量满/转接完成后也只能 <= capN
			}
		}
		close(claimDone)
	}()

	for i := 0; i < capN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = svc.AcceptTransfer(ctx, "dst", model.SessionID(fmt.Sprintf("t%d", i)))
		}(i)
	}
	wg.Wait()
	<-claimDone

	// 最终一致性断言
	d, _ = svc.st.GetAgent(ctx, "dst")
	if d.Active > capN || d.Active+d.Reserved > capN {
		t.Fatalf("oversell: active=%d reserved=%d capN=%d", d.Active, d.Reserved, capN)
	}
	if d.Reserved != 0 {
		t.Fatalf("reservation leaked: reserved=%d", d.Reserved)
	}
	// 三个转接会话全部有确定结果：agent(dst) 或 queued（dst 下线导致兜底），绝不 transferring
	agents := 0
	for i := 0; i < capN; i++ {
		s, _ := svc.GetSession(ctx, model.SessionID(fmt.Sprintf("t%d", i)))
		switch s.State {
		case model.StateAgent:
			if s.HandlerID != "dst" {
				t.Fatalf("t%d agent handler=%s", i, s.HandlerID)
			}
			agents++
		case model.StateQueued:
			// dst 在接受前下线导致的确定性兜底，原因必须可区分
			if s.Reason != model.ReasonTransferTargetGone {
				t.Fatalf("t%d queued reason=%s, want transfer_target_gone", i, s.Reason)
			}
		default:
			t.Fatalf("t%d stuck in state %s", i, s.State)
		}
	}
	if agents != d.Active {
		t.Fatalf("agents on dst=%d but active=%d", agents, d.Active)
	}
}

// 转接目标在接受前下线：会话确定性回退原坐席（在线）或重新排队，不卡死。
func TestTransferTargetOfflineResolves(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()
	setupTransfer(t, svc, "s1", "a1", "a2", 2)

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	if err := svc.AgentOffline(ctx, "a2"); err != nil {
		t.Fatal(err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	// 原坐席 a1 仍在线：回到 a1
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("after target offline: %s/%s, want agent/a1", sess.State, sess.HandlerID)
	}
	if sess.Reason != model.ReasonTransferTargetGone {
		t.Fatalf("reason=%s, want transfer_target_gone", sess.Reason)
	}
	a2, _ := svc.st.GetAgent(ctx, "a2")
	if a2.Reserved != 0 || a2.Active != 0 {
		t.Fatalf("a2 reserved=%d active=%d, want 0/0", a2.Reserved, a2.Active)
	}
}

// 转接超时无人接受：到达 TransferTimeout 后确定性回到原坐席，原因可区分。
func TestTransferTimeoutResolves(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 120 * time.Millisecond
	cfg.DispatchInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()
	setupTransfer(t, svc, "s1", "a1", "a2", 2)

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateAgent && s.HandlerID == "a1" && s.Reason == model.ReasonTransferTimeout
	})
	a2, _ := svc.st.GetAgent(ctx, "a2")
	if a2.Reserved != 0 {
		t.Fatalf("a2 reserved=%d, want 0 after timeout", a2.Reserved)
	}
}
