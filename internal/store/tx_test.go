package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// Update 失败时整体回滚：多条写入一个都不生效
func TestUpdateRollback(t *testing.T) {
	for _, makeStore := range []func() Store{
		func() Store { return NewMemStore() },
		func() Store {
			st, err := NewFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return st
		},
	} {
		st := makeStore()
		ctx := context.Background()
		if err := st.Init(ctx); err != nil {
			t.Fatal(err)
		}
		a := &model.Agent{ID: "a1", Capacity: 1, Online: true}
		if err := st.PutAgent(ctx, a); err != nil {
			t.Fatal(err)
		}

		boom := errors.New("boom")
		err := st.Update(ctx, func(tx Store) error {
			if err := tx.PutAgent(ctx, &model.Agent{ID: "a1", Capacity: 9, Online: true}); err != nil {
				return err
			}
			if err := tx.PutSession(ctx, &model.Session{ID: "s1", State: model.StateBot}); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err=%v, want boom", err)
		}

		got, err := st.GetAgent(ctx, "a1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Capacity != 1 {
			t.Fatalf("capacity=%d, want rollback to 1", got.Capacity)
		}
		if _, err := st.GetSession(ctx, "s1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("session should not exist after rollback, err=%v", err)
		}
	}
}

// Update 成功时整体生效；FileStore 下重开进程能读到完整事务结果
func TestUpdateCommitFileStore(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	err = st.Update(ctx, func(tx Store) error {
		if err := tx.PutSession(ctx, &model.Session{ID: "s1", State: model.StateQueued}); err != nil {
			return err
		}
		if err := tx.PutQueueEntry(ctx, &model.QueueEntry{SessionID: "s1", Skill: "tech"}); err != nil {
			return err
		}
		return tx.PutHandoff(ctx, &model.Handoff{ID: "h1", SessionID: "s1", Status: model.HandoffPending})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if s, err := st2.GetSession(ctx, "s1"); err != nil || s.State != model.StateQueued {
		t.Fatalf("session after reopen: %+v err=%v", s, err)
	}
	if q, err := st2.ListQueue(ctx); err != nil || len(q) != 1 {
		t.Fatalf("queue after reopen: %d err=%v", len(q), err)
	}
	if h, err := st2.GetHandoff(ctx, "h1"); err != nil || h.Status != model.HandoffPending {
		t.Fatalf("handoff after reopen: %+v err=%v", h, err)
	}
	_ = st2.Close()
	_ = fstest.MapFS{}
	_ = filepath.Separator
}
