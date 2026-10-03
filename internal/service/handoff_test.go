package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// 机器人 -> 排队 -> 坐席接管全流程，消息不丢不重不乱序
func TestHandoffFlowAndMessageOrdering(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	// 机器人阶段消息
	if _, err := svc.SendMessage(ctx, "s1", "u1", "bot-msg-1"); err != nil {
		t.Fatal(err)
	}

	registerAgent(t, svc, "a1", []string{"tech"}, 2, true)
	ch, err := svc.Events(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	// 排队期间消息
	if _, err := svc.SendMessage(ctx, "s1", "u1", "queued-msg-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SendMessage(ctx, "s1", "u1", "queued-msg-3"); err != nil {
		t.Fatal(err)
	}

	// 坐席收到抢占邀请并接管
	ev := waitEvent(t, ch, 2*time.Second)
	if ev.Type != model.EventOffer {
		t.Fatalf("expected offer, got %s", ev.Type)
	}
	if _, err := svc.Claim(ctx, "a1", "s1"); err != nil {
		t.Fatal(err)
	}

	// 接管后消息
	if _, err := svc.SendMessage(ctx, "s1", "u1", "agent-msg-4"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SendMessage(ctx, "s1", "u1", "agent-msg-5"); err != nil {
		t.Fatal(err)
	}

	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("session should be handled by a1, got %s/%s", sess.State, sess.HandlerID)
	}

	msgs, err := svc.ListMessages(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if got := strSeq(msgs); fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("seqs = %v, want [1 2 3 4 5]", got)
	}
	// 消息归属：1 归机器人，2-5 归坐席
	for _, m := range msgs {
		want := "agent"
		if m.Seq == 1 {
			want = "bot"
		}
		if m.HandlerType != want {
			t.Fatalf("msg %d handler = %s, want %s", m.Seq, m.HandlerType, want)
		}
		if m.Seq >= 2 && (!m.Delivered || m.HandlerID != "a1") {
			t.Fatalf("msg %d not delivered to a1", m.Seq)
		}
	}

	// 坐席邮箱：排队积压 2,3 + 接管后 4,5，严格有序
	evs := collectEvents(ch, 200*time.Millisecond)
	if got := evSeqs(evs); fmt.Sprint(got) != "[2 3 4 5]" {
		t.Fatalf("agent got seqs = %v, want [2 3 4 5]", got)
	}

	// 交接单可追溯
	hs, _ := svc.ListHandoffs(ctx, "s1")
	if len(hs) != 2 {
		t.Fatalf("handoffs = %d, want 2", len(hs))
	}
	statuses := map[string]bool{}
	for _, h := range hs {
		if h.Status != model.HandoffCompleted {
			t.Fatalf("handoff %s status = %s", h.ID, h.Status)
		}
		statuses[h.FromType+"->"+h.ToType] = true
	}
	if !statuses["bot->queue"] || !statuses["queue->agent"] {
		t.Fatalf("missing handoff records: %v", statuses)
	}
}

// 多坐席并发抢占同一会话：结果确定，唯一胜者
func TestConcurrentClaimsSingleWinner(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	// 5 个坐席同时在线抢占
	for i := 0; i < 5; i++ {
		registerAgent(t, svc, fmt.Sprintf("a%d", i), []string{"tech"}, 2, true)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := []string{}
	conflicts := 0
	otherErrs := 0
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := model.AgentID(fmt.Sprintf("a%d", i))
			// 等邀请广播
			time.Sleep(time.Duration(i) * time.Millisecond)
			_, err := svc.Claim(ctx, id, "s1")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners = append(winners, string(id))
			} else if errors.Is(err, ErrConflict) {
				conflicts++
			} else {
				otherErrs++
			}
		}(i)
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly 1", winners)
	}
	if conflicts != 4 || otherErrs != 0 {
		t.Fatalf("conflicts=%d otherErrs=%d, want 4/0", conflicts, otherErrs)
	}
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != winners[0] {
		t.Fatalf("session handler = %s/%s, want %s", sess.State, sess.HandlerID, winners[0])
	}
	// 胜者容量 +1，其余坐席容量不变
	agents, _ := svc.ListAgents(ctx)
	for _, a := range agents {
		want := 0
		if string(a.ID) == winners[0] {
			want = 1
		}
		if a.Active != want {
			t.Fatalf("agent %s active = %d, want %d", a.ID, a.Active, want)
		}
	}
	// 审计：1 条允许 + 4 条拒绝
	audits, _ := svc.ListAudit(ctx, "s1")
	allowed, denied := 0, 0
	for _, e := range audits {
		if e.Action == "offer.claim" {
			if e.Allowed {
				allowed++
			} else {
				denied++
			}
		}
	}
	if allowed != 1 || denied != 4 {
		t.Fatalf("audit claim allowed=%d denied=%d, want 1/4", allowed, denied)
	}
}

// 越权接管必须被拒绝且审计
func TestClaimPermissionDenied(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	if _, err := svc.StartSession(ctx, "s1", "tech"); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a1", []string{"finance"}, 2, true) // 技能不符
	registerAgent(t, svc, "a2", []string{"tech"}, 2, true)    // 容量为0
	if err := svc.st.PutAgent(context.Background(), &model.Agent{
		ID: "a2", Name: "a2", Role: model.RoleAgent, Skills: []string{"tech"},
		Capacity: 0, Online: true, UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	registerAgent(t, svc, "a3", []string{"tech"}, 2, false) // 离线

	if _, err := svc.RequestHandoff(ctx, "s1", model.ReasonHandoffRequested); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 等分发

	if _, err := svc.Claim(ctx, "a1", "s1"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("skill mismatch: err = %v, want ErrPermissionDenied", err)
	}
	if _, err := svc.Claim(ctx, "a2", "s1"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("capacity full: err = %v, want ErrPermissionDenied", err)
	}
	if _, err := svc.Claim(ctx, "a3", "s1"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("offline: err = %v, want ErrPermissionDenied", err)
	}
	// 会话仍在排队，未被接管
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateQueued && sess.State != model.StateOffering {
		t.Fatalf("session state = %s, want queued/offering", sess.State)
	}
	audits, _ := svc.ListAudit(ctx, "s1")
	denied := 0
	for _, e := range audits {
		if e.Action == "offer.claim" && !e.Allowed {
			denied++
		}
	}
	if denied != 3 {
		t.Fatalf("denied audits = %d, want 3", denied)
	}
}

// 多个会话并发消息：每会话序号严格连续，不串会话
func TestConcurrentMessagesOrdering(t *testing.T) {
	svc := newTestService(t, testConfig(), nil)
	ctx := context.Background()

	for _, id := range []string{"s1", "s2", "s3"} {
		if _, err := svc.StartSession(ctx, model.SessionID(id), "tech"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for _, id := range []string{"s1", "s2", "s3"} {
		for n := 0; n < 20; n++ {
			wg.Add(1)
			go func(id string, n int) {
				defer wg.Done()
				_, _ = svc.SendMessage(ctx, model.SessionID(id), "u", fmt.Sprintf("%s-%d", id, n))
			}(id, n)
		}
	}
	wg.Wait()
	for _, id := range []string{"s1", "s2", "s3"} {
		msgs, err := svc.ListMessages(ctx, model.SessionID(id))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 20 {
			t.Fatalf("%s messages = %d, want 20", id, len(msgs))
		}
		seqs := strSeq(msgs)
		if !sort.SliceIsSorted(seqs, func(i, j int) bool { return seqs[i] < seqs[j] }) {
			t.Fatalf("%s seqs not sorted: %v", id, seqs)
		}
		for i, m := range msgs {
			if m.Seq != int64(i+1) {
				t.Fatalf("%s seq gap: %v", id, seqs)
			}
		}
	}
}
