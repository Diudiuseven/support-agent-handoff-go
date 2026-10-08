package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// startCrashServer 构建并以独立进程启动 server，返回 baseURL 与进程
func startCrashServer(t *testing.T, dir string, port int) (string, *os.Process) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "server")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/server")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"HANDOFF_DATA_DIR="+dir,
		"HANDOFF_SERVER_ADDR=127.0.0.1:"+strconv.Itoa(port),
		"HANDOFF_DISPATCH_INTERVAL=20ms",
		"HANDOFF_OFFER_TIMEOUT=200ms",
		"HANDOFF_QUEUE_TIMEOUT=30s",
		"HANDOFF_TRANSFER_TIMEOUT=5s",
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 50*time.Millisecond); err == nil {
			_ = c.Close()
			return base, cmd.Process
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatal("server did not become ready")
	return "", nil
}

func crashPost(t *testing.T, base, path string, body any) []byte {
	t.Helper()
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	resp, err := http.Post(base+path, "application/json", &buf)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("POST %s status=%d body=%s", path, resp.StatusCode, data)
	}
	return data
}

// crashPostTry 返回状态码而非 Fatal，用于允许冲突后重试
func crashPostTry(t *testing.T, base, path string, body any) int {
	t.Helper()
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	resp, err := http.Post(base+path, "application/json", &buf)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func crashGet(t *testing.T, base, path string, wantStatus int) []byte {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s status=%d want %d body=%s", path, resp.StatusCode, wantStatus, data)
	}
	return data
}

func waitForOffering(t *testing.T, base, sid string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/sessions/" + sid)
		if err != nil {
			t.Fatal(err)
		}
		var s model.Session
		_ = json.NewDecoder(resp.Body).Decode(&s)
		resp.Body.Close()
		if s.State == model.StateOffering {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s never reached offering", sid)
}

func claimUntilOK(t *testing.T, base, agent, sid string) {
	t.Helper()
	waitForOffering(t, base, sid)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := crashPostTry(t, base, "/agents/"+agent+"/claim",
			map[string]string{"session_id": sid})
		if st == http.StatusOK {
			return
		}
		// 409 表示邀请超时正在重试，重新等待 offering
		waitForOffering(t, base, sid)
	}
	t.Fatalf("claim %s by %s never succeeded", sid, agent)
}

// 反复"操作 -> SIGKILL 强杀 -> 重启"，每轮验证关键不变量：
//   - 无 transferring 中间态；每个 agent 名下会话数 == 服务端 Active 且 <= Capacity；
//   - 交接单无残留 pending，agent 态会话必有指向当前处理方的完成单；
//   - 消息序号连续、不重复。
//
// SIGKILL 模拟进程在任意时刻（包括落盘过程中）被终止。
func TestCrashRestartInvariants(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test builds a binary; skip in -short")
	}
	dir := t.TempDir()
	port := freePort(t)
	base, proc := startCrashServer(t, dir, port)

	crashPost(t, base, "/agents", map[string]any{"id": "a1", "role": "agent", "skills": []string{"tech"}, "capacity": 20})
	crashPost(t, base, "/agents/a1/online", nil)
	crashPost(t, base, "/agents", map[string]any{"id": "a2", "role": "agent", "skills": []string{"tech"}, "capacity": 20})
	crashPost(t, base, "/agents/a2/online", nil)

	const rounds = 6
	for round := 0; round < rounds; round++ {
		sid := fmt.Sprintf("s%d", round)
		crashPost(t, base, "/sessions", map[string]string{"id": sid, "skill": "tech"})
		crashPost(t, base, "/sessions/"+sid+"/handoff", nil)
		claimUntilOK(t, base, "a1", sid)
		for m := 0; m < 3; m++ {
			crashPost(t, base, "/sessions/"+sid+"/messages",
				map[string]string{"from": "u", "body": fmt.Sprintf("%s-m%d", sid, m)})
		}
		// 由当前真正的处理方发起转接（崩溃重启后归属可能变化）
		handler := currentHandler(t, base, sid)
		crashPost(t, base, "/agents/"+handler+"/transfer",
			map[string]string{"session_id": sid, "to_agent": "a2", "actor_id": handler, "role": "agent"})
		if round%2 == 0 {
			// 偶数轮：接受完成
			if st := crashPostTry(t, base, "/agents/a2/transfer/accept",
				map[string]string{"session_id": sid}); st != http.StatusNoContent {
				t.Fatalf("accept status=%d round=%d", st, round)
			}
		}
		// 奇数轮：停留在 transferring 时被 SIGKILL，验证重启自动裁决

		if err := proc.Signal(os.Kill); err != nil {
			t.Fatal(err)
		}
		_, _ = proc.Wait()

		port = freePort(t)
		base, proc = startCrashServer(t, dir, port)
		assertGlobalInvariants(t, base)
	}

	// 全部会话最终可处于确定态；消息序号连续不重复
	for round := 0; round < rounds; round++ {
		sid := fmt.Sprintf("s%d", round)
		var msgs []*model.Message
		if err := json.Unmarshal(crashGet(t, base, "/sessions/"+sid+"/messages", http.StatusOK), &msgs); err != nil {
			t.Fatal(err)
		}
		for i, m := range msgs {
			if m.Seq != int64(i+1) {
				t.Fatalf("session %s seq gap at %d: got %d", sid, i, m.Seq)
			}
		}
	}
	_ = proc.Signal(os.Kill)
	_, _ = proc.Wait()
}

func currentHandler(t *testing.T, base, sid string) string {
	t.Helper()
	resp, err := http.Get(base + "/sessions/" + sid)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s model.Session
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s.HandlerID
}

// assertGlobalInvariants 通过 HTTP 校验崩溃重启后的全局一致性
func assertGlobalInvariants(t *testing.T, base string) {
	t.Helper()
	var sessions []*model.Session
	if err := json.Unmarshal(crashGet(t, base, "/sessions", http.StatusOK), &sessions); err != nil {
		t.Fatal(err)
	}
	var agents []*model.Agent
	if err := json.Unmarshal(crashGet(t, base, "/agents", http.StatusOK), &agents); err != nil {
		t.Fatal(err)
	}
	capByID := map[string]int{}
	activeReported := map[string]int{}
	for _, a := range agents {
		capByID[string(a.ID)] = a.Capacity
		activeReported[string(a.ID)] = a.Active
	}
	held := map[string]int{}
	for _, s := range sessions {
		if s.State == model.StateTransferring {
			t.Fatalf("crash left session %s in transferring", s.ID)
		}
		if s.State == model.StateAgent {
			if s.HandlerID == "" {
				t.Fatalf("agent session %s has empty handler", s.ID)
			}
			held[s.HandlerID]++
		}
	}
	for id, n := range held {
		if n > capByID[id] {
			t.Fatalf("agent %s oversold after crash: holds %d > cap %d", id, n, capByID[id])
		}
		if n != activeReported[id] {
			t.Fatalf("agent %s active=%d but holds %d after crash", id, activeReported[id], n)
		}
	}
	for _, s := range sessions {
		if s.State != model.StateAgent {
			continue
		}
		var hs []*model.Handoff
		if err := json.Unmarshal(crashGet(t, base, "/sessions/"+string(s.ID)+"/handoffs", http.StatusOK), &hs); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, h := range hs {
			if h.Status == model.HandoffPending {
				t.Fatalf("session %s has pending handoff after recovery", s.ID)
			}
			if h.Status == model.HandoffCompleted && h.ToID == s.HandlerID {
				found = true
			}
		}
		if !found {
			t.Fatalf("session %s handled by %s but no completed handoff record", s.ID, s.HandlerID)
		}
	}
}
