# 配置项与一致性模型

## 配置项（`engine.Options`）

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `QueueMaxWait` | `time.Duration` | `2m` | 会话在队列中的最长等待时间。**0 表示不启用超时回退**。到期后由 sweep 以 `queue_timeout` 回退机器人 |
| `DeliveryBuffer` | `int` | `64` | `NewDefaultInboxes` 生成的**每个** inbox 的缓冲深度。自管 inbox 可自行决定容量 |
| `SweepInterval` | `duration` | `1s` | 后台扫描周期：处理超时队列、机会式分配空闲坐席、重试被满 inbox 拒绝的投递 |

示例：

```go
opts := engine.Options{
    QueueMaxWait:   90 * time.Second,
    DeliveryBuffer: 128,
    SweepInterval:  250 * time.Millisecond,
}
```

## 坐席配置（`agent.Profile`）

| 字段 | 说明 |
|      |      |
| `ID` | 坐席唯一标识（同时用于 RBAC 身份与 inbox key `agent:<ID>`） |
| `Name` | 展示名 |
| `Queues` | 该坐席能服务的技能队列集合 |
| `Capacity` | 最大并发会话数；`<=0` 按 1 计。`Load == 已预留的不同 session 数` |

### 坐席选择的确定性规则

当需要自动分配（升级时立即分配 / 上线触发 / sweep）时，在所有
**在线 + 服务该队列 + 有余量** 的坐席中按 `(Load 升序, ID 升序)` 取第一个：
负载最低者优先，同负载按 ID 固定排序——所以无论多少线程并发触发分配，
选择结果都是确定的。

## 队列策略（`queue.Policy` / `queue.Entry`）

- 全局 FIFO（`container/list`），`Head(queue)` 在全局顺序上按技能过滤，
  保证跨队列也是确定顺序。
- `Entry.Deadline = EnqueuedAt + QueueMaxWait`；`DueTimeouts` 一次性
  摘除所有到期项（最旧的在前），引擎据此回退并记录原因。
- 重复 `Enqueue` 同一会话会刷新截止时间并移到队尾（显式重新入队语义）。

## RBAC 权限（`auth.RBACPolicy`）

| 动作 | system | admin | agent |
|------|--------|-------|-------|
| `session.escalate`（升级） | ✅ | ✅ | ❌ |
| `session.take`（抢单） | ✅ | ✅ | 仅当目标坐席 == 自己 |
| `session.accept`（接手转接） | ❌ | ✅ | 仅当目标坐席 == 自己 |
| `session.transfer`（发起转接） | ❌ | ✅ | 需引擎校验是当前 owner（资源级） |
| `session.cancel_transfer` | ❌ | ✅ | owner 或目标可取消 |
| `session.close` | ✅ | ✅ | assigned 时仅 owner |
| `session.send` | ✅ | ✅ | agent 消息校验归属，出站需为当前 owner |

两层防御：

1. **RBAC 粗粒度**（角色 + 身份匹配）；
2. **引擎资源级**（当前状态、当前 owner、容量、在线状态）。

两层拒绝都写审计（`outcome=denied`），原因分别为 `unauthorized:<sid>`
与具体错误（如 `session is already assigned`）。

## 审计记录（`audit.Record`）

```json
{
  "time": "2026-10-03T10:45:34Z",
  "actor": "alice",
  "action": "session.take",
  "session": "cust-1001",
  "target": "alice",
  "outcome": "allowed",
  "reason": "agent_accepted",
  "detail": ""
}
```

- `outcome`：`allowed` / `ignored`（幂等空操作，如重复升级已在排队的会话）/ `denied`；
- 可多 sink 并行写入（`MemorySink` 便于测试与查询，`FileSink` JSONL+fsync 持久化）；
- 一个 sink 写失败不影响其他 sink，错误聚合返回。

## 一致性模型总结

**串行化边界**

- 每 session 独立互斥锁：同会话的升级/抢单/转接/关闭/收发消息完全串行；
- 不同 session 使用不同锁，不共享临界区。

**原子提交协议**（`engine.mutate`）

1. 锁住 session，读投影并校验权限/状态；
2. 预留外部资源（坐席容量、摘队列），同时准备回滚闭包；
3. 以 `expectedLastSeq` 原子追加事件批次（连续 seq）；
4. 冲突 → 回滚外部副作用 → 从存储重载投影 → 有限次重试；
5. 提交点之后：折叠事件 → 所有权变更时重绑未确认消息 → 摘队列/释放容量 →
   收集投递 → 解锁 → 非阻塞投递 → 写审计。

**投递语义**

- 至少一次（at-least-once）+ 每会话严格顺序；
- 先持久化后投递，崩溃只会导致重投，不会丢；
- 重投打 `Redelivered` 标记，消费端按消息 ID 幂等即可做到 effectively-once；
- inbox 满时消息保持 due，由 sweep 重试，**不阻塞其他 session**。

**容量不变式**

- 一个 session 任一时刻最多占用一个坐席的一个容量槽；
- 转接窗口同时占原 owner 与目标两个槽（防止双方都超卖），
  `accept` 释放原 owner，`cancel/close/return` 释放全部相关槽；
- 所有释放路径都按 `(agent, session)` 幂等，重复执行安全。

## 常见失败的可解释结果

| 场景 | API 结果 | Session 结果 | 审计原因 |
|------|---------|-------------|---------|
| 坐席抢单慢了一步 | `ErrAlreadyAssigned` | 赢家 assigned | denied: session is already assigned |
| 目标坐席满载 | `ErrAgentAtCapacity` | 状态不变 | denied: agent is at capacity |
| 坐席离线 | 升级时入队；服务中自动回退 | queued / bot | agent_offline |
| 排队超时 | 无（sweep 自动） | bot | queue_timeout |
| 非 owner 发起转接 | `ErrUnauthorized` | 状态不变 | unauthorized / denied |
| 伪造身份抢单 | `ErrUnauthorized` | 状态不变 | unauthorized |
| 重复 ack | `nil`（幂等） | 无新事件 | 不重复处理 |
