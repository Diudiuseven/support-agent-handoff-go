package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/service"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.DispatchInterval = 10 * time.Millisecond
	cfg.OfferTimeout = 200 * time.Millisecond
	cfg.QueueTimeout = 500 * time.Millisecond
	svc := service.New(cfg, store.NewMemStore())
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	return httptest.NewServer(NewHandler(svc).Routes())
}

func post(t *testing.T, url string, body interface{}) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHTTPEndToEnd(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	base := srv.URL

	// 注册坐席并上线
	resp := post(t, base+"/agents", map[string]interface{}{
		"id": "a1", "name": "agent-one", "role": "agent",
		"skills": []string{"tech"}, "capacity": 2,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register agent: %d", resp.StatusCode)
	}
	resp.Body.Close()
	if resp := post(t, base+"/agents/a1/online", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("online: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 建会话 -> 转人工 -> 发消息
	resp = post(t, base+"/sessions", map[string]string{"id": "s1", "skill": "tech"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("start session: %d", resp.StatusCode)
	}
	resp.Body.Close()
	if resp := post(t, base+"/sessions/s1/handoff", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("handoff: %d", resp.StatusCode)
	}
	resp.Body.Close()
	if resp := post(t, base+"/sessions/s1/messages", map[string]string{"from": "u", "body": "hello"}); resp.StatusCode != http.StatusCreated {
		t.Fatalf("message: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 等邀请后抢占
	time.Sleep(100 * time.Millisecond)
	resp = post(t, base+"/agents/a1/claim", map[string]string{"session_id": "s1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("claim: %d", resp.StatusCode)
	}
	var h model.Handoff
	json.NewDecoder(resp.Body).Decode(&h)
	resp.Body.Close()
	if h.Status != model.HandoffCompleted {
		t.Fatalf("handoff status = %s", h.Status)
	}

	// 会话状态
	resp, _ = http.Get(base + "/sessions/s1")
	var sess model.Session
	json.NewDecoder(resp.Body).Decode(&sess)
	resp.Body.Close()
	if sess.State != model.StateAgent || sess.HandlerID != "a1" {
		t.Fatalf("session = %s/%s", sess.State, sess.HandlerID)
	}

	// 越权接管返回 409（会话已被接管）
	resp = post(t, base+"/agents/a1/claim", map[string]string{"session_id": "s1"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("double claim status = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()

	// 审计可查
	resp, _ = http.Get(base + "/sessions/s1/audit")
	var audits []*model.AuditEvent
	json.NewDecoder(resp.Body).Decode(&audits)
	resp.Body.Close()
	if len(audits) == 0 {
		t.Fatal("no audit events")
	}

	// 队列可查
	resp, _ = http.Get(base + "/queue")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("queue: %d", resp.StatusCode)
	}
	resp.Body.Close()
}
