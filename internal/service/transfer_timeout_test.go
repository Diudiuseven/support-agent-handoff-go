package service

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 转接发起后目标坐席始终不接受也不拒绝：到达 Deadline 后系统自动裁决，
// 回退在线原坐席，原因 transfer_timeout，会话不悬空、计数准确。
func TestTransferTimeoutBackToOrigin(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 100 * time.Millisecond
	cfg.DispatchInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	// a2 什么都不做，等待系统自动裁决
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateAgent && s.HandlerID == "a1" && s.Reason == model.ReasonTransferTimeout
	})
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.Reason != model.ReasonTransferTimeout {
		t.Fatalf("reason = %s", sess.Reason)
	}
	// 转接单失败
	hs, _ := svc.ListHandoffs(ctx, "s1")
	for _, h := range hs {
		if h.FromID == "a1" && h.ToID == "a2" && h.Status != model.HandoffFailed {
			t.Fatalf("transfer handoff status = %s, want failed", h.Status)
		}
	}
	// 容量准确
	a1, _ := svc.st.GetAgent(ctx, "a1")
	a2, _ := svc.st.GetAgent(ctx, "a2")
	if a1.Active != 1 || a2.Active != 0 {
		t.Fatalf("active: a1=%d a2=%d, want 1/0", a1.Active, a2.Active)
	}
	// 原坐席收到通知；且可再次正常转接
	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	if err := svc.AcceptTransfer(ctx, "a2", "s1"); err != nil {
		t.Fatal(err)
	}
}

// 转接超时且原坐席已下线：自动重新排队继续分发。
func TestTransferTimeoutRequeueWhenOriginOffline(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 2 * time.Second
	cfg.DispatchInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	// 原坐席在转接等待期间下线：会话立刻回排队（agent_offline 优先）
	if err := svc.AgentOffline(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateQueued && sess.State != model.StateOffering {
		t.Fatalf("state = %s, want queued/offering", sess.State)
	}
	// a3 上线接管
	registerAgent(t, svc, "a3", []string{"tech"}, 2, true)
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})
	if _, err := svc.Claim(ctx, "a3", "s1"); err != nil {
		t.Fatal(err)
	}
	sess, _ = svc.GetSession(ctx, "s1")
	if sess.HandlerID != "a3" {
		t.Fatalf("handler = %s, want a3", sess.HandlerID)
	}
	// 原 pending 转接单已失败
	hs, _ := svc.ListHandoffs(ctx, "s1")
	for _, h := range hs {
		if h.FromID == "a1" && h.ToID == "a2" && h.Status != model.HandoffFailed {
			t.Fatalf("stale transfer status = %s", h.Status)
		}
	}
}
