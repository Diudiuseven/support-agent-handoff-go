# Support Agent Handoff (Go)

客服会话在 **机器人 (bot)** 与 **人工坐席 (agent)** 之间的交接、排队与
分发引擎。解决转人工后无人接手、消息错投、坐席靠人工协调的问题。

## 能力一览

| # | 能力 | 实现要点 |
|---|------|----------|
| 1 | 机器人 ↔ 人工原子可追溯交接 | 事件溯源（append-only），每次转移是一个原子事件批次；消息先持久化再投递，未确认消息随 owner 切换重投 |
| 2 | 多坐席竞争同一 session | 每 session 一把互斥锁 + 坐席容量原子预留（`TryReserve`），竞争结果确定且唯一赢家 |
| 3 | 排队与容量控制 | 全局 FIFO、按技能队列 `Head`、最大等待时间；超时回退机器人，原因码可区分 |
| 4 | 上下线与转接 | 转接期间消息仍归属原 owner，`Accept` 后未 ack 消息按序重绑给新 owner；坐席下线自动安全回退 |
| 5 | 持久化与重启恢复 | `FileStore` JSONL + fsync；重放事件重建状态，排队/转接中/未确认消息全部恢复且不重复处理 |
| 6 | 并发一致性 | session 间互不阻塞；乐观序列号 + 冲突重试；投递非阻塞，慢坐席只给自己背压 |
| 7 | 权限与审计 | RBAC（system/agent/admin）+ 资源级 owner 校验；允许、忽略、拒绝三类审计全程留痕 |
| 8 | 自动化测试与文档 | 并发接管、转接顺序、超时升级、FileStore 重启恢复等场景，详见 [docs/](docs/) |

## 快速开始

```go
es := store.NewMemoryStore()                    // 或 store.NewFileStore(ctx, "./data")
reg := agent.NewRegistry()
_ = reg.Register(agent.Profile{ID: "alice", Queues: []domain.QueueName{"billing"}, Capacity: 2})
reg.SetOnline("alice", true)

inboxes, botIB, agentIBs := engine.NewDefaultInboxes(64, "alice")
eng := engine.New(es, reg, queue.NewFIFO(), auth.NewRBACPolicy(),
    audit.NewLogger(audit.NewMemorySink()), inboxes, engine.DefaultOptions())
_ = eng.Run(ctx)
defer eng.Stop()

sys := auth.Principal{ID: "bot", Role: auth.RoleSystem}
_ = eng.Open(ctx, "cust-1", "billing", time.Now())

res, _ := eng.Escalate(ctx, sys, "cust-1", domain.ReasonCustomerRequested, time.Now())
// res.Status == StatusAssigned, res.AssignedAgent == "alice"（有空闲坐席立即分配，否则入队）
```

可运行示例：

```bash
go run ./cmd/handoff-demo
```

重启恢复（新进程）：

```go
eng2 := engine.New(es2, reg2, queue.NewFIFO(), auth.NewRBACPolicy(), aud, inboxes, opts)
if err := eng2.Recover(ctx, time.Now()); err != nil { /* ... */ }
```

## 目录结构

```
domain/   核心类型：Session/Message/Handler/Event/状态/原因码/错误
auth/     RBAC 权限策略（system / agent / admin）
store/    事件存储：MemoryStore（测试）、FileStore（JSONL + fsync，生产可用）
agent/    坐席注册表：上线状态、容量、按 session 幂等预留
queue/    FIFO 等待队列：截止时间、按队列头、确定性顺序
audit/    审计：MemorySink / FileSink，允许/忽略/拒绝三类结果
engine/   编排核心：交接、抢占、转接、消息路由、超时扫描、重启恢复
docs/     状态机、配置项、一致性模型文档
```

## 关键语义

- **单一处理方**：任一时刻一个 session 只有一个 owner（bot 或某个 agent）。
  竞争接管由「session 锁 + 容量预留 + 事件原子追加」三重保证唯一赢家。
- **消息不丢**：入站消息先追加 `delivered` 事件再向 inbox 非阻塞投递；
  inbox 满或 owner 变更时消息保持 due，后台扫描重投。
- **消息不重（处理侧）**：投递是 **at-least-once + 每会话有序**；
  重投消息带 `Redelivered=true`，handler 必须用消息 ID 做幂等。
  `Ack` 幂等，重复 ack 不会产生重复事件。
- **转接窗口**：`TransferTo` 后状态为 `transferring`，owner 不变，
  消息继续给原坐席；`Accept` 是所有权唯一切换点，未 ack 消息按原顺序
  重绑并投给新坐席；`CancelTransfer` 或关闭/回退会释放目标预留。
- **慢坐席隔离**：inbox 是有界缓冲，`TryDeliver` 永远不阻塞；
  某坐席处理慢只会延迟自己 session 的推送，不影响其他 session 的分发。

## 测试

```bash
go test ./...                 # 全部
go test -race ./...           # 含竞态检测
go test -run Recover ./engine # 重启恢复相关
```

更多细节见 [`docs/STATE_MACHINE.md`](docs/STATE_MACHINE.md) 与
[`docs/CONFIGURATION.md`](docs/CONFIGURATION.md)。
