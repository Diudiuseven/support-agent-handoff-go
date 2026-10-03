package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

func setupHandledSession(t *testing.T, svc *Service, sessionID, agentID string, skills []string) {
	t.Helper()
	ctx := context.Background()
	if _, err := svc.StartSession(ctx, model.SessionID(sessionID), skills[0]); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, agentID, skills, 2, true)
	if _, err := svc.RequestHandoff(ctx, model.SessionID(sessionID), model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, model.SessionID(sessionID))
		return err == nil && (s.State == model.StateOffering || s.State == model.StateQueued)
	})
	if _, err := svc.Claim(ctx, model.AgentID(agentID), model.SessionID(sessionID)); err != nil {
		t.Fatal(err)
	}
}

// 转接接受：处理方原子切换，消息按顺序归属正确方
func TestTransferAccept(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
	ch2, _ := svc.Events(ctx, "a2")

	// 转接前消息（归 a1）
	if _, err := svc.SendMessage(ctx, "s1", "u", "before-transfer"); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateTransferring || sess.HandlerID != "a1" {
		t.Fatalf("during transfer: state=%s handler=%s, want transferring/a1", sess.State, sess.HandlerID)
	}

	// 转接期间消息仍归原坐席 a1
	if _, err := svc.SendMessage(ctx, "s1", "u", "during-transfer"); err != nil {
		t.Fatal(err)
	}
	msgs, _ := svc.ListMessages(ctx, "s1")
	for _, m := range msgs {
		if m.Seq == 2 && (m.HandlerID != "a1" || !m.Delivered) {
			t.Fatalf("during-transfer msg handler=%s delivered=%v", m.HandlerID, m.Delivered)
		}
	}

	// 非目标坐席不能接受
	if err := svc.AcceptTransfer(ctx, "a3", "s1"); err == nil {
		t.Fatal("a3 should not be able to accept")
	}

	if err := svc.AcceptTransfer(ctx, "a2", "s1"); err != nil {
		t.Fatal(err)
	}
	sess, _ = svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a2" {
		t.Fatalf("after accept: state=%s handler=%s, want agent/a2", sess.State, sess.HandlerID)
	}

	// 转接后消息归 a2
	if _, err := svc.SendMessage(ctx, "s1", "u", "after-transfer"); err != nil {
		t.Fatal(err)
	}
	msgs, _ = svc.ListMessages(ctx, "s1")
	for _, m := range msgs {
		if m.Seq == 3 && (m.HandlerID != "a2" || !m.Delivered) {
			t.Fatalf("after-transfer msg handler=%s delivered=%v", m.HandlerID, m.Delivered)
		}
	}

	// 容量切换：a1 释放，a2 占用
	agents, _ := svc.ListAgents(ctx)
	for _, a := range agents {
		switch string(a.ID) {
		case "a1":
			if a.Active != 0 {
				t.Fatalf("a1 active = %d, want 0", a.Active)
			}
		case "a2":
			if a.Active != 1 {
				t.Fatalf("a2 active = %d, want 1", a.Active)
			}
		}
	}

	// a2 收到转接邀请 + 转接后消息，顺序正确
	evs := collectEvents(ch2, 300*time.Millisecond)
	hasOffer, hasMsg := false, false
	for _, ev := range evs {
		if ev.Type == model.EventOffer {
			hasOffer = true
		}
		if ev.Type == model.EventMessage && ev.Message != nil && ev.Message.Seq == 3 {
			hasMsg = true
		}
	}
	if !hasOffer || !hasMsg {
		t.Fatalf("a2 events: offer=%v msg3=%v", hasOffer, hasMsg)
	}

	// 交接单可追溯：a1->a2 完成
	hs, _ := svc.ListHandoffs(ctx, "s1")
	found := false
	for _, h := range hs {
		if h.FromID == "a1" && h.ToID == "a2" && h.Status == model.HandoffCompleted {
			found = true
		}
	}
	if !found {
		t.Fatalf("transfer handoff not completed: %+v", hs)
	}
}

// 转接拒绝：回退原坐席，原因可区分
func TestTransferReject(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)

	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}
	if err := svc.RejectTransfer(ctx, "a2", "s1"); err != nil {
		t.Fatal(err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("after reject: state=%s handler=%s, want agent/a1", sess.State, sess.HandlerID)
	}
	if sess.Reason != model.ReasonTransferRejected {
		t.Fatalf("reason = %s, want transfer_rejected", sess.Reason)
	}
	// 原坐席容量仍在
	a1, _ := svc.st.GetAgent(ctx, "a1")
	if a1.Active != 1 {
		t.Fatalf("a1 active = %d, want 1", a1.Active)
	}
}

// 非处理方发起转接被拒绝
func TestTransferPermissionDenied(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
	registerAgent(t, svc, "a3", []string{"tech"}, 2, true)

	// a3 不是处理方，不能转接 s1
	if _, err := svc.Transfer(ctx, "a3", model.RoleAgent, "s1", "a2"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	// 转接给离线坐席被拒
	if err := svc.AgentOffline(ctx, "a3"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a3"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("transfer to offline: err = %v, want ErrPermissionDenied", err)
	}
}

// 坐席离线后其会话重新排队，可被其他坐席接管
func TestAgentOfflineRequeue(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	if err := svc.AgentOffline(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateQueued || sess.HandlerID != "" || sess.Reason != model.ReasonAgentOffline {
		t.Fatalf("after offline: state=%s handler=%s reason=%s", sess.State, sess.HandlerID, sess.Reason)
	}
	a1, _ := svc.st.GetAgent(ctx, "a1")
	if a1.Active != 0 {
		t.Fatalf("a1 active = %d, want 0", a1.Active)
	}
	q, _ := svc.ListQueue(ctx)
	if len(q) != 1 {
		t.Fatalf("queue len = %d, want 1", len(q))
	}

	// 新坐席上线后收到邀请并抢占接管
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s1")
		return err == nil && s.State == model.StateOffering
	})
	if _, err := svc.Claim(ctx, "a2", "s1"); err != nil {
		t.Fatal(err)
	}
	// 消息不丢：排队期间消息在接管后补投
	ch, _ := svc.Events(ctx, "a2")
	if _, err := svc.SendMessage(ctx, "s1", "u", "while-queued"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, ch, 2*time.Second)
}

// 坐席并发服务多个会话互不干扰
func TestAgentConcurrentSessions(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	registerAgent(t, svc, "a1", []string{"tech"}, 3, true)
	for i := 0; i < 3; i++ {
		id := model.SessionID(fmt.Sprintf("s%d", i))
		if _, err := svc.StartSession(ctx, id, "tech"); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.RequestHandoff(ctx, id, model.ReasonHandoffRequested); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 2*time.Second, func() bool {
		s, err := svc.GetSession(ctx, "s0")
		return err == nil && s.State == model.StateOffering
	})
	for i := 0; i < 3; i++ {
		id := model.SessionID(fmt.Sprintf("s%d", i))
		if _, err := svc.Claim(ctx, "a1", id); err != nil {
			t.Fatal(err)
		}
	}
	// 3 个会话都在 a1 名下，状态正确互不干扰
	for i := 0; i < 3; i++ {
		s, _ := svc.GetSession(ctx, model.SessionID(fmt.Sprintf("s%d", i)))
		if s.State != model.StateAgent || s.HandlerID != "a1" {
			t.Fatalf("s%d state=%s handler=%s", i, s.State, s.HandlerID)
		}
	}
	// 容量控制：第 4 个会话排队等待
	if _, err := svc.StartSession(ctx, "s3", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s3", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	s, _ := svc.GetSession(ctx, "s3")
	if s.State != model.StateQueued && s.State != model.StateOffering {
		t.Fatalf("s3 state = %s, want queued/offering (capacity full)", s.State)
	}
}
