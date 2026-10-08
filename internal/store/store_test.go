package store

import (
	"context"
	"testing"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

func TestMemTxCommitRollback(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	a := &model.Agent{ID: "a1", Capacity: 2}
	if err := m.PutAgent(ctx, a); err != nil {
		t.Fatal(err)
	}

	// 事务 A 提交：可见
	tx, _ := m.BeginTx(ctx)
	got, err := tx.GetAgent(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	got.Active = 1
	if err := tx.PutAgent(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if a, _ := m.GetAgent(ctx, "a1"); a.Active != 1 {
		t.Fatalf("after commit active=%d, want 1", a.Active)
	}

	// 事务 B 回滚：不可见
	tx2, _ := m.BeginTx(ctx)
	g2, _ := tx2.GetAgent(ctx, "a1")
	g2.Active = 5
	_ = tx2.PutAgent(ctx, g2)
	tx2.Rollback()
	if a, _ := m.GetAgent(ctx, "a1"); a.Active != 1 {
		t.Fatalf("after rollback active=%d, want 1", a.Active)
	}

	// 隔离性：未提交的事务彼此不可见
	tx3, _ := m.BeginTx(ctx)
	g3, _ := tx3.GetAgent(ctx, "a1")
	g3.Active = 9
	_ = tx3.PutAgent(ctx, g3)
	if a, _ := m.GetAgent(ctx, "a1"); a.Active != 1 {
		t.Fatalf("parent saw uncommitted tx data: active=%d", a.Active)
	}
	tx3.Rollback()
}

func TestFileTxAtomicityAndPersist(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sess := &model.Session{ID: "s1", State: model.StateBot, HandlerType: "bot"}
	if err := f.PutSession(ctx, sess); err != nil {
		t.Fatal(err)
	}
	msg := &model.Message{ID: "s1-1", SessionID: "s1", Seq: 1, Delivered: false}
	if err := f.AppendMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// 事务内更新 Delivered=true 并提交：磁盘必须持久化该标记
	tx, _ := f.BeginTx(ctx)
	ms, _ := tx.ListMessages(ctx, "s1")
	ms[0].Delivered = true
	if err := tx.UpdateMessage(ctx, ms[0]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 重新从磁盘加载，标记仍在
	f2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := f2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := f2.ListMessages(ctx, "s1")
	if len(got) != 1 || !got[0].Delivered {
		t.Fatalf("Delivered flag not persisted across restart: %+v", got)
	}

	// 事务回滚：磁盘不留痕
	tx2, _ := f2.BeginTx(ctx)
	cur, _ := tx2.GetSession(ctx, "s1")
	cur.State = model.StateQueued
	_ = tx2.PutSession(ctx, cur)
	tx2.Rollback()
	f3, _ := NewFileStore(dir)
	_ = f3.Init(ctx)
	c3, _ := f3.GetSession(ctx, "s1")
	if c3.State != model.StateBot {
		t.Fatalf("rollback leaked to disk: state=%s", c3.State)
	}
}
