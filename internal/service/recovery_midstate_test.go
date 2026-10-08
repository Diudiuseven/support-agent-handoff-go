package service

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 模拟"接受转接"在落盘过程中被中断后可能留下的各种中间态，
// 重启后必须看到要么交接前、要么交接后的完整状态，计数与单据一致。
//
// 场景：s1 原在 a1 名下，a1 转接到 a2。构造 a2 已为目标、
// 会话已切到 agent/a2 但交接单仍 pending 的中间态。
func TestRecoverMidAcceptState(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
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

	// 直接构造崩溃中间态：会话已到 a2，交接单仍 pending（模拟接受途中断电）
	now := time.Now()
	sess, _ := svc.GetSession(ctx, "s1")
	sess.State = model.StateAgent
	sess.HandlerID = "a2"
	sess.UpdatedAt = now
	_ = svc.st.PutSession(ctx, sess)
	_ = svc.st.PutHandoff(ctx, &model.Handoff{
		ID:        "h-transfer",
		SessionID: "s1",
		FromType:  "agent",
		FromID:    "a1",
		ToType:    "agent",
		ToID:      "a2",
		Reason:    model.ReasonTransferred,
		Status:    model.HandoffPending,
		CreatedAt: now,
	})
	// 计数停留在交接前：a1=1,a2=0
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	s, _ := svc2.GetSession(ctx, "s1")
	if s.State != model.StateAgent || s.HandlerID != "a2" {
		t.Fatalf("session after recover = %s/%s, want agent/a2 (post-handoff view)", s.State, s.HandlerID)
	}
	// 交接单补完成，全程可追溯
	var h *model.Handoff
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	for _, x := range hs {
		if x.ID == "h-transfer" {
			h = x
		}
	}
	if h == nil || h.Status != model.HandoffCompleted {
		t.Fatalf("transfer handoff = %+v, want completed", h)
	}
	// 计数与归属一致：a2 恰好 1，a1 为 0，无预留泄漏
	a1, _ := svc2.st.GetAgent(ctx, "a1")
	a2, _ := svc2.st.GetAgent(ctx, "a2")
	if a1.Active != 0 || a2.Active != 1 || a1.Reserved != 0 || a2.Reserved != 0 {
		t.Fatalf("counts after recover: a1(active=%d,res=%d) a2(active=%d,res=%d), want 0/0 1/0",
			a1.Active, a1.Reserved, a2.Active, a2.Reserved)
	}
}

// 崩溃在"发起转接后、接受前"：会话 transferring、交接单 pending、a2 有预留。
// 重启后转接单失败、预留释放、会话重新排队继续推进，且不重复处理。
func TestRecoverMidTransferReleasesReservation(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
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
	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	s, _ := svc2.GetSession(ctx, "s1")
	if s.State != model.StateQueued {
		t.Fatalf("state = %s, want queued", s.State)
	}
	a2, _ := svc2.st.GetAgent(ctx, "a2")
	if a2.Reserved != 0 || a2.Active != 0 {
		t.Fatalf("a2 active=%d reserved=%d, want 0/0", a2.Active, a2.Reserved)
	}
	a1, _ := svc2.st.GetAgent(ctx, "a1")
	if a1.Active != 0 {
		t.Fatalf("a1 active=%d, want 0 after crash requeue", a1.Active)
	}
	q, _ := svc2.ListQueue(ctx)
	if len(q) != 1 {
		t.Fatalf("queue len=%d, want 1", len(q))
	}

	// 幂等：再触发一次恢复不产生重复队列条目/重复单据
	if err := svc2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	q, _ = svc2.ListQueue(ctx)
	if len(q) != 1 {
		t.Fatalf("queue after second recover len=%d, want 1", len(q))
	}
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	completedToQueue := 0
	for _, x := range hs {
		if x.FromType == "agent" && x.ToType == "queue" && x.Status == model.HandoffCompleted {
			completedToQueue++
		}
	}
	if completedToQueue != 1 {
		t.Fatalf("agent->queue completed handoffs = %d, want 1", completedToQueue)
	}
}

// 会话已是 agent/a1 但缺少任何交接记录（单据丢失）：重启补登，审计可追溯。
func TestRecoverMissingHandoffRecord(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	// 手工构造 agent 态会话但无交接单
	sess, _ := svc.GetSession(ctx, "s1")
	sess.State = model.StateAgent
	sess.HandlerType = "agent"
	sess.HandlerID = "a1"
	_ = svc.st.PutSession(ctx, sess)
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	found := false
	for _, h := range hs {
		if h.ToID == "a1" && h.Status == model.HandoffCompleted {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing rebuilt handoff record: %+v", hs)
	}
	audits, _ := svc2.ListAudit(ctx, "s1")
	rebuilt := false
	for _, e := range audits {
		if e.Action == "recover" {
			rebuilt = true
		}
	}
	if !rebuilt {
		t.Fatal("missing recover audit")
	}
	a1, _ := svc2.st.GetAgent(ctx, "a1")
	if a1.Active != 1 {
		t.Fatalf("a1 active=%d, want 1", a1.Active)
	}
}
