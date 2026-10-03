# 会话状态机

一个 session 的所有状态都由 **事件日志**（`domain.Event`）重放得到，
内存中没有任何"只能存在于内存"的关键状态。因此：

- 每个转移都可解释、可审计（事件带 `Actor` / `Reason`）；
- 重启后重放即可完全恢复；
- 并发转移通过每-session 串行化 + 乐观序列号（expectedLastSeq）收敛到
  唯一确定结果。

## 状态定义（`domain.SessionStatus`）

| 状态 | 含义 | 当前 owner |
|------|------|-----------|
| `bot` | 机器人处理中（初始态） | bot |
| `queued` | 已升级，等待人工坐席 | 无（等待分配） |
| `assigned` | 唯一人工坐席服务中 | 某一个 agent |
| `transferring` | 坐席间转接中（瞬态） | 原 agent；TransferTo 已为目标预留容量 |
| `closed` | 会话结束（终态） | 无，容量已释放 |

## 转移与事件

```
                 Open
                  │
                  ▼
     ┌──────────────────────┐
     │         bot          │◄──────────────┐
     └──────────────────────┘               │
        │ Escalate（无可用坐席）             │ ReturnToBot
        ▼                                    │ (queue_timeout /
     ┌──────────────────────┐               │  agent_offline /
     │        queued        │               │  no_agent_available)
     └──────────────────────┘               │
        │ Take(坐席抢) / 调度器分配           │
        │ [EvAgentAssigned]                  │
        ▼                                    │
     ┌──────────────────────┐               │
     │       assigned       │───────────────┘
     │ (owner = agent A)    │
     └──────────────────────┘
        │ TransferTo(B) [EvTransferStarted]
        ▼
     ┌──────────────────────┐
     │    transferring      │  owner 仍是 A，目标 B 的容量已预留
     │ (TransferTo = B)     │  期间消息继续投递给 A
     └──────────────────────┘
        │ Accept(B)            │ CancelTransfer
        │ [EvTransferCompleted]│ [EvTransferCanceled]
        ▼                      ▼
     assigned(B)            assigned(A)
        │
        │ Close [EvSessionClosed]
        ▼
      closed
```

任意非终态都可以被 `ReturnToBot`（超时/坐席下线/无可用坐席）或 `Close` 终止。

## 事件清单（`domain.EventType`）

| 事件 | 触发操作 | 关键字段 |
|------|---------|---------|
| `session_opened` | `Open` | Queue |
| `escalated_to_queue` | `Escalate`（无空闲坐席） | Queue, Reason |
| `agent_assigned` | `Escalate` 立即分配 / `Take` / 调度器自动分配 | AgentID, Queue, Reason |
| `returned_to_bot` | 队列超时、坐席下线、恢复时坐席不可用 | Reason（区分原因） |
| `transfer_started` | `TransferTo` | AgentID=目标 |
| `transfer_completed` | `Accept` | AgentID=新 owner |
| `transfer_canceled` | `CancelTransfer` | AgentID=目标 |
| `delivered` | `Send`（每条消息先持久化） | Msg |
| `acked` | handler 确认 / 出站消息自动确认 | Msg.ID |
| `session_closed` | `Close` | Reason |

每个事件都有：

- `Seq`：session 内连续递增（从 0 开始），乐观并发的依据；
- `At`：事件时间（持久化时补齐）；
- `Actor`：触发者（system / scheduler / 坐席 ID / admin）；
- `Reason`：见下表的**处理原因**。

## 可区分的处理原因（`domain.ReasonCode`）

| 原因码 | 含义 |
|--------|------|
| `customer_requested` | 客户主动要求转人工 |
| `bot_escalated` | 机器人判断需升级 |
| `agent_accepted` | 坐席接管成功（抢单或自动分配） |
| `queue_timeout` | **排队超时**，回退机器人 |
| `agent_offline` | **坐席下线**（运行时或恢复时），安全回退 |
| `no_agent_available` | 无可用坐席（预留策略） |
| `agent_at_capacity` | 目标坐席满载 |
| `agent_transferred` | 转接（发起/完成） |
| `agent_rejected` | 转接被取消 |
| `customer_ended` / `agent_ended` | 客户/坐席结束会话 |
| `unauthorized` | 越权操作（RBAC 或 owner 校验拒绝） |
| `restart_recovered` | 重启恢复产生的动作 |

## 消息归属规则

- 消息的路由目标 = **持久化时/投递时**的当前 owner，并按 session 有序。
- `Send` 先追加 `delivered` 事件，再非阻塞投递，杜绝"交接窗口丢消息"。
- 所有权变更（assigned / transfer_completed / returned_to_bot）后：
  - 所有未 ack 的消息重绑到新 owner；
  - 标记 `Redelivered=true`；
  - 按原始到达顺序重投。
- `ack` 只认消息 ID，重复确认是无副作用的成功。
- 出站消息（bot/agent 回复）会持久化并立即自动 ack，**不会**被投回 bot 自己。

## 重启恢复（`Engine.Recover`）

`ReadAll` → 按 session 分组 → `materialize` 重放，然后：

| 重放后的状态 | 恢复动作 |
|-------------|---------|
| `bot` | 无额外动作，未 ack 消息重投给 bot |
| `queued` | 重新入队，尽量沿用原等待截止时间；坐席上线后立即触发自动分配 |
| `assigned` | 对原坐席做幂等的容量再预留；坐席离线/不存在/满载则回退 bot（`agent_offline`） |
| `transferring` | 同时为原 owner 与目标恢复预留；任一不可用则安全回退 bot |
| 任意状态 | 未 ack 消息标记为重投并推送（有 inbox 时），不产生重复 `acked` |

恢复操作是幂等的：容量预留按 `(agent, session)` 去重，ack 按消息 ID 去重。
