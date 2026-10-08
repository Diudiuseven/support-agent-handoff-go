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

// 场景1：转接发起后、目标接受前，目标坐席的余量恰好被别的会话占满。
// 接受必须有确定结果：会话回退原坐席（仍在线），原因 transfer_target_full，
// 不能长时间停留在 transferring。
func TestTransferAcceptWhenTargetCapacityFilled(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 5 * time.Second // 本测试不靠超时，靠接受时仲裁
	svc := newTestService(t, cfg, nil)
	ctx := context.Background()

	// a1 持有 s1；a2 容量 1
	setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
	registerAgent(t, svc, "a2", []string{"tech"}, 1, true)
	registerAgent(t, svc, "a3", []string{"tech"}, 2, true)

	// a1 把 s1 转给 a2（此时 a2 有余量）
	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2"); err != nil {
		t.Fatal(err)
	}

	// 在 a2 接受之前，a2 先接管另一个会话 s2，占满其容量
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
	a2, _ := svc.st.GetAgent(ctx, "a2")
	if a2.Active != a2.Capacity {
		t.Fatalf("precondition: a2 active=%d cap=%d", a2.Active, a2.Capacity)
	}

	// a2 尝试接受转接：必须失败且给出可区分原因，但会话不能卡在 transferring
	err := svc.AcceptTransfer(ctx, "a2", "s1")
	if !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("accept err = %v, want ErrAgentUnavailable", err)
	}

	// 会话确定性地回退原坐席 a1
	sess, _ := svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("after failed accept: state=%s handler=%s, want agent/a1", sess.State, sess.HandlerID)
	}
	if sess.Reason != model.ReasonTransferTargetFull {
		t.Fatalf("reason = %s, want transfer_target_full", sess.Reason)
	}
	// 转接单失败，可追溯
	var failed *model.Handoff
	hs, _ := svc.ListHandoffs(ctx, "s1")
	for _, h := range hs {
		if h.FromID == "a1" && h.ToID == "a2" {
			failed = h
		}
	}
	if failed == nil || failed.Status != model.HandoffFailed {
		t.Fatalf("transfer handoff should be failed: %+v", failed)
	}
	// 容量准确：a1 仍持有 s1，a2 只持有 s2，无超卖
	a1, _ := svc.st.GetAgent(ctx, "a1")
	a2, _ = svc.st.GetAgent(ctx, "a2")
	if a1.Active != 1 || a2.Active != 1 {
		t.Fatalf("active counts: a1=%d a2=%d, want 1/1", a1.Active, a2.Active)
	}
	// a3 仍有容量，原坐席可再次发起转接并成功
	if _, err := svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a3"); err != nil {
		t.Fatal(err)
	}
	if err := svc.AcceptTransfer(ctx, "a3", "s1"); err != nil {
		t.Fatalf("accept by a3 with free capacity: %v", err)
	}
	sess, _ = svc.GetSession(ctx, "s1")
	if sess.State != model.StateAgent || sess.HandlerID != "a3" {
		t.Fatalf("final state = %s/%s, want agent/a3", sess.State, sess.HandlerID)
	}
}

// 场景2：发起转接与目标余量被占满"同时"发生（并发，重复 30 轮）。
// 无论谁先获得锁，最终结果确定且容量不超卖：要么转接成功（a2 拿到 s1），
// 要么 a2 容量被占、会话安全回退 a1；不会两边都计数或会话悬空。
func TestTransferConcurrentWithCapacityFill(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 5 * time.Second
	for iter := 0; iter < 30; iter++ {
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			svc := newTestService(t, cfg, nil)
			ctx := context.Background()
			setupHandledSession(t, svc, "s1", "a1", []string{"tech"})
			registerAgent(t, svc, "a2", []string{"tech"}, 1, true)

			// 让 s2 排队并已向 a2 发起邀请，处于可被 a2 抢占状态
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

			var wg sync.WaitGroup
			wg.Add(2)
			var transferErr error
			go func() { // 发起转接（容量检查）
				defer wg.Done()
				_, transferErr = svc.Transfer(ctx, "a1", model.RoleAgent, "s1", "a2")
			}()
			go func() { // 同时 a2 抢占 s2（占满容量）
				defer wg.Done()
				_, _ = svc.Claim(ctx, "a2", "s2")
			}()
			wg.Wait()

			if transferErr != nil {
				// 转接发起时容量已满被拒：s1 仍在 a1，确定终态
				sess, _ := svc.GetSession(ctx, "s1")
				if sess.State != model.StateAgent || sess.HandlerID != "a1" {
					t.Fatalf("transfer rejected but s1 = %s/%s", sess.State, sess.HandlerID)
				}
			} else {
				// 转接发起成功：接受时做最终仲裁
				err := svc.AcceptTransfer(ctx, "a2", "s1")
				sess, _ := svc.GetSession(ctx, "s1")
				if err == nil {
					if sess.HandlerID != "a2" || sess.State != model.StateAgent {
						t.Fatalf("accepted but s1 = %s/%s", sess.State, sess.HandlerID)
					}
				} else {
					if !errors.Is(err, ErrAgentUnavailable) {
						t.Fatalf("accept err = %v", err)
					}
					if sess.State != model.StateAgent || sess.HandlerID != "a1" {
						t.Fatalf("accept failed but s1 = %s/%s, want agent/a1", sess.State, sess.HandlerID)
					}
				}
			}

			assertCapacityInvariants(t, svc, ctx)
		})
	}
}

// 场景3：多个目标坐席并发接受转接，同时有其他会话并发抢占——
// 全局仲裁保证每个坐席 Active<=Capacity、处理方唯一、总数守恒、无转接悬空。
func TestConcurrentAcceptAndClaimCapacityArbitration(t *testing.T) {
	cfg := testConfig()
	cfg.TransferTimeout = 5 * time.Second
	const n = 6
	for iter := 0; iter < 10; iter++ {
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			svc := newTestService(t, cfg, nil)
			ctx := context.Background()

			// 6 个源坐席各持有 1 个会话（容量给足）
			for i := 0; i < n; i++ {
				setupHandledSession(t, svc, fmt.Sprintf("s%d", i), fmt.Sprintf("src%d", i), []string{"tech"})
			}
			// 3 个目标坐席，每个容量 2，合计 6，正好可承接全部转接
			registerAgent(t, svc, "t1", []string{"tech"}, 2, true)
			registerAgent(t, svc, "t2", []string{"tech"}, 2, true)
			registerAgent(t, svc, "t3", []string{"tech"}, 2, true)

			// 每个会话随机转给 t1/t2/t3
			targets := []string{"t1", "t2", "t3"}
			for i := 0; i < n; i++ {
				src := model.AgentID(fmt.Sprintf("src%d", i))
				to := model.AgentID(targets[i%3])
				if _, err := svc.Transfer(ctx, src, model.RoleAgent, model.SessionID(fmt.Sprintf("s%d", i)), to); err != nil {
					t.Fatal(err)
				}
			}

			// 三个目标坐席并发接受他们各自被指派的会话
			var wg sync.WaitGroup
			accept := func(agent string) {
				defer wg.Done()
				for i := 0; i < n; i++ {
					if targets[i%3] != agent {
						continue
					}
					_ = svc.AcceptTransfer(ctx, model.AgentID(agent), model.SessionID(fmt.Sprintf("s%d", i)))
				}
			}
			wg.Add(3)
			go accept("t1")
			go accept("t2")
			go accept("t3")
			wg.Wait()

			sessions, _ := svc.ListSessions(ctx)
			handled := map[string]int{}
			for _, s := range sessions {
				if s.State == model.StateTransferring {
					t.Fatalf("session %s stuck transferring", s.ID)
				}
				if s.State == model.StateAgent {
					handled[s.HandlerID]++
				}
			}
			for _, ag := range targets {
				a, err := svc.st.GetAgent(ctx, model.AgentID(ag))
				if err != nil {
					t.Fatal(err)
				}
				if a.Active > a.Capacity {
					t.Fatalf("%s oversold: active=%d cap=%d", ag, a.Active, a.Capacity)
				}
				if a.Active != handled[ag] {
					t.Fatalf("%s active=%d but actually holds %d", ag, a.Active, handled[ag])
				}
			}
			assertCapacityInvariants(t, svc, ctx)
		})
	}
}

// assertCapacityInvariants 校验全局容量不变量：
// 每个坐席 Active==实际名下处理中会话数，且不超过 Capacity；无 transferring 悬空。
func assertCapacityInvariants(t *testing.T, svc *Service, ctx context.Context) {
	t.Helper()
	agents, err := svc.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := svc.ListSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]int{}
	for _, s := range sessions {
		if s.State == model.StateTransferring {
			t.Fatalf("session %s stuck in transferring", s.ID)
		}
		if s.State == model.StateAgent || s.State == model.StateTransferring {
			held[s.HandlerID]++
		}
	}
	for _, a := range agents {
		if a.Active > a.Capacity {
			t.Fatalf("agent %s oversold: active=%d cap=%d", a.ID, a.Active, a.Capacity)
		}
		if a.Active != held[string(a.ID)] {
			t.Fatalf("agent %s active=%d but holds %d sessions", a.ID, a.Active, held[string(a.ID)])
		}
	}
}
