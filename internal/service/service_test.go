package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

func testConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.DataDir = ""
	cfg.DispatchInterval = 10 * time.Millisecond
	cfg.OfferTimeout = 200 * time.Millisecond
	cfg.QueueTimeout = 500 * time.Millisecond
	cfg.OfferRetries = 1
	cfg.MailboxSize = 8
	cfg.DefaultCapacity = 2
	return cfg
}

func newTestService(t *testing.T, cfg config.Config, st store.Store) *Service {
	t.Helper()
	if st == nil {
		st = store.NewMemStore()
	}
	svc := New(cfg, st)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("run service: %v", err)
	}
	t.Cleanup(svc.Stop)
	return svc
}

func registerAgent(t *testing.T, svc *Service, id string, skills []string, cap int, online bool) *model.Agent {
	t.Helper()
	a := &model.Agent{
		ID:       model.AgentID(id),
		Name:     id,
		Role:     model.RoleAgent,
		Skills:   skills,
		Capacity: cap,
		Online:   online,
	}
	if err := svc.RegisterAgent(context.Background(), a); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if online {
		if err := svc.AgentOnline(context.Background(), a.ID); err != nil {
			t.Fatalf("agent online: %v", err)
		}
	}
	return a
}

func waitEvent(t *testing.T, ch <-chan *model.Event, timeout time.Duration) *model.Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for event")
	}
	return nil
}

func collectEvents(ch <-chan *model.Event, d time.Duration) []*model.Event {
	var out []*model.Event
	deadline := time.After(d)
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
}

func eventually(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.After(d)
	for {
		if fn() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("condition not met within %v", d)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func strSeq(msgs []*model.Message) []int64 {
	out := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Seq)
	}
	return out
}

func evSeqs(evs []*model.Event) []int64 {
	out := make([]int64, 0, len(evs))
	for _, ev := range evs {
		if ev.Type == model.EventMessage && ev.Message != nil {
			out = append(out, ev.Message.Seq)
		}
	}
	return out
}

var _ = fmt.Sprintf
