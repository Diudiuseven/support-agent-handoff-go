package service

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

func newFileBackedService(t *testing.T, cfg config.Config) (*Service, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(cfg, st)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	return svc, dir
}

func restartService(t *testing.T, dir string, cfg config.Config) *Service {
	t.Helper()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(cfg, st)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	return svc
}

// 重启恢复：人工处理中的会话不中断，消息序号连续不重复
func TestRecoverAgentSession(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
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
	if _, err := svc.SendMessage(ctx, "s1", "u", "before-restart"); err != nil {
		t.Fatal(err)
	}

	// 重启
	svc.Stop()
	svc2 := restartService(t, dir, cfg)

	sess, err := svc2.GetSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("recovered session = %s/%s, want agent/a1", sess.State, sess.HandlerID)
	}
	a1, _ := svc2.st.GetAgent(ctx, "a1")
	if a1.Active != 1 {
		t.Fatalf("a1 active = %d, want 1", a1.Active)
	}
	// 交接单可追溯
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	if len(hs) != 2 {
		t.Fatalf("handoffs = %d, want 2", len(hs))
	}
	// 消息序号连续，继续推进不重复
	if _, err := svc2.SendMessage(ctx, "s1", "u", "after-restart"); err != nil {
		t.Fatal(err)
	}
	msgs, _ := svc2.ListMessages(ctx, "s1")
	if len(msgs) != 2 || msgs[1].Seq != 2 {
		t.Fatalf("messages after recovery: %+v", msgs)
	}
}

// 重启恢复：排队中的会话继续分发
func TestRecoverQueuedSession(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	sess, err := svc2.GetSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.State != model.StateQueued {
		t.Fatalf("recovered state = %s, want queued", sess.State)
	}
	q, _ := svc2.ListQueue(ctx)
	if len(q) != 1 {
		t.Fatalf("queue entries = %d, want 1", len(q))
	}

	// 新坐席上线后继续分发并接管
	registerAgent(t, svc2, "a1", []string{"tech"}, 2, true)
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc2.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})
	if _, err := svc2.Claim(ctx, "a1", "s1"); err != nil {
		t.Fatal(err)
	}
	sess, _ = svc2.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("after claim: %s/%s", sess.State, sess.HandlerID)
	}
}

// 重启恢复：未完成的转接回退原坐席（原坐席仍在线）；原坐席离线则重新排队
func TestRecoverTransferringSession(t *testing.T) {
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
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateTransferring {
		t.Fatalf("precondition: state = %s", sess.State)
	}
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	sess, err := svc2.GetSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	// 原坐席 a1 仍在线：转接崩溃后确定性回退原坐席，会话不悬空
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("recovered state = %s/%s, want agent/a1", sess.State, sess.HandlerID)
	}
	// 未完成的转接单标记失败
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	var transfer *model.Handoff
	for _, h := range hs {
		if h.FromID == "a1" && h.ToID == "a2" {
			transfer = h
		}
	}
	if transfer == nil {
		t.Fatalf("transfer handoff not found: %+v", hs)
	}
	if transfer.Status != model.HandoffFailed {
		t.Fatalf("pending transfer status = %s, want failed", transfer.Status)
	}
	// 容量仍准确：a1 持有 1 个，a2 为 0
	if a1, _ := svc2.st.GetAgent(ctx, "a1"); a1.Active != 1 {
		t.Fatalf("a1 active = %d, want 1", a1.Active)
	}
	if a2, _ := svc2.st.GetAgent(ctx, "a2"); a2.Active != 0 {
		t.Fatalf("a2 active = %d, want 0", a2.Active)
	}
	// 队列中不应有该会话（会话回到 a1 名下）
	if q, _ := svc2.ListQueue(ctx); len(q) != 0 {
		t.Fatalf("queue = %d, want 0", len(q))
	}
}

// 重启恢复：未完成转接且原坐席已离线 → 重新排队继续推进
func TestRecoverTransferringSessionRequeue(t *testing.T) {
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
	// 原坐席在转接期间下线（与转接交织）：会话回到排队（可能已被快速重新邀请）
	if err := svc.AgentOffline(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if s, _ := svc.GetSession(ctx, "s1"); s.State != model.StateQueued && s.State != model.StateOffering {
		t.Fatalf("after offline want queued/offering, got %s", s.State)
	}
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	sess, err := svc2.GetSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.State != model.StateQueued {
		t.Fatalf("recovered state = %s, want queued", sess.State)
	}
	q, _ := svc2.ListQueue(ctx)
	if len(q) != 1 {
		t.Fatalf("queue = %d, want 1", len(q))
	}
	// 新坐席上线后继续分发并接管
	registerAgent(t, svc2, "a3", []string{"tech"}, 2, true)
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc2.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})
	if _, err := svc2.Claim(ctx, "a3", "s1"); err != nil {
		t.Fatal(err)
	}
	sess, _ = svc2.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a3" {
		t.Fatalf("after claim: %s/%s", sess.State, sess.HandlerID)
	}
}

// 重启恢复：崩溃在"状态已切换但交接单未完成"之间，恢复后补完成交接单。
// 通过直接注入一个"半完成"磁盘快照来模拟任意时刻被中断（绕过事务）。
func TestRecoverPendingHandoffCompletion(t *testing.T) {
	cfg := testConfig()
	svc, dir := newFileBackedService(t, cfg)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, _ := svc.GetSession(ctx, "s1")
		return s.State == model.StateOffering
	})

	// 构造半完成快照：会话已在 agent/a1，但交接单仍 pending
	sess, _ := svc.GetSession(ctx, "s1")
	sess.State = model.StateAgent
	sess.HandlerType = "agent"
	sess.HandlerID = "a1"
	sess.OfferID = ""
	a1, _ := svc.st.GetAgent(ctx, "a1")
	now := time.Now()
	fs := svc.st.(*store.FileStore)
	if err := fs.LoadSnapshotForTest(store.SnapshotData{
		Sessions: map[model.SessionID]*model.Session{"s1": sess},
		Agents:   map[model.AgentID]*model.Agent{"a1": a1},
		Handoffs: map[string]*model.Handoff{
			"handoff-pending": {
				ID:        "handoff-pending",
				SessionID: "s1",
				FromType:  "queue",
				ToType:    "agent",
				ToID:      "a1",
				Reason:    model.ReasonClaim,
				Status:    model.HandoffPending,
				CreatedAt: now,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	svc.Stop()

	svc2 := restartService(t, dir, cfg)
	hs, _ := svc2.ListHandoffs(ctx, "s1")
	found := false
	for _, h := range hs {
		if h.ID == "handoff-pending" && h.Status == model.HandoffCompleted {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending handoff not completed after recovery: %+v", hs)
	}
	a1r, _ := svc2.st.GetAgent(ctx, "a1")
	if a1r.Active != 1 {
		t.Fatalf("a1 active = %d, want 1", a1r.Active)
	}
}
