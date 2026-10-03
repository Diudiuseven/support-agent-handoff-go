# support-agent-handoff-go

客服会话在**机器人与人工坐席**之间交接、排队分发与抢占接管的 Go 实现。
核心保证：会话在任一时刻只有一个处理方；交接前后消息不丢、不重、不乱序；
多坐席抢占结果确定；坐席满载时排队，超时按策略升级或回退机器人；
支持上下线、转接、越权拒绝、全流程审计与重启恢复。

## 会话状态机

```
                         RequestHandoff
  ┌───────┐  (bot→queue)   ┌────────┐  分发邀请   ┌──────────┐
  │  bot  │ ───────────────► │ queued │ ──────────► │ offering │
  └───┬───┘                 └───┬────┘            └────┬─────┘
      │                         │ ▲                    │
      │ 排队超时/fallback       │ │ 邀请超时/重试       │ 坐席抢占 Claim
      │ (queue_timeout)        │ └────────────────────│ (先到先得)
      │                         │ 升级 escalate         ▼
      │                         │ (skill=senior)   ┌────────┐
      │                         │                 │ agent  │◄────────┐
      │                         │                 └───┬────┘         │
      │                         │                     │ Transfer     │ Accept
      │                         │                     ▼              │
      │                         │                 ┌──────────────┐  │
      │                         │                 │ transferring │──┘
      │                         │                 └──────┬───────┘
      │                         │                        │ Reject
      │                         │                        ▼
      │                         │                   回退原坐席 / 重新排队
      │                         ▼
      └────────────────── 坐席下线(agent_offline) → 重新排队
```

状态说明：

| 状态 | 含义 |
| --- | --- |
| `bot` | 机器人处理中（初始状态） |
| `queued` | 排队等待人工；坐席下线、转接被拒后也回到此状态 |
| `offering` | 已向符合条件的在线坐席广播抢占邀请，等待第一个抢占者 |
| `agent` | 人工坐席处理中（同时只能有一个坐席） |
| `transferring` | 坐席发起转接，等待目标坐席接受；期间原坐席仍是处理方 |
| `ended` | 会话结束（终态） |

关键不变量：

- **单一处理方**：只有 `agent` 状态持有 `HandlerID`；`queued/offering` 处理方为空；
  `transferring` 期间处理方仍为原坐席，接受后原子切换。
- **消息有序**：每条消息在全局锁内分配单调递增序号 `Seq`，并按当前处理方归属；
  排队期间的消息挂起（`Delivered=false`），坐席接管后补投；交接与消息在同一
  临界区内排序，消息要么在交接前归旧方，要么在交接后归新方。
- **抢占确定**：并发 `Claim` 在全局锁上串行化，第一个满足条件的坐席成功，
  其余得到 `ErrConflict`，结果可解释、可审计。
- **慢坐席不阻塞**：邀请与消息投递均为非阻塞邮箱（缓冲满则保留在持久化层，
  可通过 `ListOffers/ListMessages` 拉取），分发循环永不等待慢坐席。

## 排队与容量控制

- 坐席注册时声明 `Capacity`（默认 5），`Active` 为当前处理中会话数；
  `Active >= Capacity` 时不再收到邀请，新会话进入排队。
- 队列按**优先级降序、排队时间升序（FIFO）**分发；坐席上线、容量释放、
  接管/结束都会唤醒分发循环。
- 每个抢占邀请有有效期（`OfferTimeout`），超时未抢占则重新排队并重试
  （`OfferRetries`）；重试耗尽或排队总超时（`QueueTimeout`）时执行既定策略：
  - `escalate`（默认）：升级到高级技能组（`EscalationSkill`，默认 `senior`）、
    优先级 +1，继续排队；已升级过仍无坐席则回退机器人；
  - `fallback`：回退机器人继续处理。
- 处理原因可区分：`handoff_requested`、`claim`、`offer_timeout`、
  `queue_timeout`、`agent_offline`、`transferred`、`transfer_rejected`、
  `escalated`、`session_ended`、`recovered`。

## 权限与审计

- `Claim`：坐席/主管角色；坐席技能组须匹配（主管可跨组）；在线且有余量。
- `Transfer`：仅当前处理坐席或主管；目标坐席须在线、有余量、技能匹配。
- `EndSession`：坐席只能结束自己处理的会话。
- 越权操作返回 `ErrPermissionDenied`（HTTP 403）并写入 `allowed=false` 的
  审计事件；所有关键动作（建会话、转人工、邀请、抢占、转接、接受、拒绝、
  上下线、结束、升级、恢复）均有只追加审计记录，可按会话追溯。

## 持久化与重启恢复

- `Store` 接口有两个实现：`MemStore`（纯内存）与 `FileStore`。
- `FileStore` 在每次写操作后把全量状态序列化，以**临时文件 + fsync +
  rename + 目录 fsync** 原子落盘，崩溃不会产生半写快照。
- 重启后 `Recover` 幂等推进：
  - `offering` → 回退排队（邀请已丢失），重新分发；
  - `transferring` → 未完成转接单置失败，会话回退排队；
  - `agent` 但交接单 `pending` → 补完成交接单（状态切换与单据落盘之间崩溃）；
    缺少交接记录则补登，保证可追溯；
  - `queued` → 补齐排队条目，保留剩余超时时间；
  - 重算坐席 `Active` 计数，修正崩溃漂移；
  - 在线坐席名下的积压消息补投。

## 配置项

| 字段 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `data_dir` | `HANDOFF_DATA_DIR` | 空（内存） | 持久化目录；非空时使用 FileStore |
| `queue_timeout` | `HANDOFF_QUEUE_TIMEOUT` | `5m` | 排队总超时 |
| `offer_timeout` | `HANDOFF_OFFER_TIMEOUT` | `30s` | 抢占邀请有效期 |
| `offer_retries` | `HANDOFF_OFFER_RETRIES` | `2` | 邀请无响应重试次数 |
| `default_policy` | `HANDOFF_DEFAULT_POLICY` | `escalate` | `escalate` / `fallback` |
| `dispatch_interval` | `HANDOFF_DISPATCH_INTERVAL` | `200ms` | 分发扫描间隔 |
| `mailbox_size` | `HANDOFF_MAILBOX_SIZE` | `256` | 坐席邮箱缓冲（慢坐席不阻塞） |
| `default_capacity` | `HANDOFF_DEFAULT_CAPACITY` | `5` | 坐席默认并发容量 |
| `escalation_skill` | `HANDOFF_ESCALATION_SKILL` | `senior` | 升级目标技能组 |
| `server_addr` | `HANDOFF_SERVER_ADDR` | `:8080` | HTTP 监听地址 |

可通过 `HANDOFF_CONFIG=/path/to/config.json` 加载 JSON 配置，环境变量覆盖。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sessions` | 创建会话 `{id, skill}` |
| GET | `/sessions` | 会话列表 |
| GET | `/sessions/{id}` | 会话详情 |
| POST | `/sessions/{id}/handoff` | 机器人请求转人工 |
| POST | `/sessions/{id}/messages` | 发送消息 `{from, body}` |
| GET | `/sessions/{id}/messages` | 消息记录 |
| POST | `/sessions/{id}/end` | 结束会话 |
| GET | `/sessions/{id}/audit` | 审计记录 |
| GET | `/sessions/{id}/handoffs` | 交接单记录 |
| POST | `/agents` | 注册坐席 `{id, name, role, skills, capacity}` |
| GET | `/agents` | 坐席列表 |
| POST | `/agents/{id}/online` | 上线 |
| POST | `/agents/{id}/offline` | 下线（会话重新排队） |
| POST | `/agents/{id}/claim` | 抢占会话 `{session_id}` |
| POST | `/agents/{id}/transfer` | 发起转接 `{session_id, to_agent}` |
| POST | `/agents/{id}/transfer/accept` | 接受转接 |
| POST | `/agents/{id}/transfer/reject` | 拒绝转接 |
| GET | `/queue` | 排队队列 |

## 运行与测试

```bash
go build ./...
go test -race ./...
go run ./cmd/server   # 默认 :8080，纯内存；设置 HANDOFF_DATA_DIR 持久化
```

测试覆盖：交接全流程与消息顺序、多坐席并发抢占（唯一胜者）、越权接管拒绝、
转接接受/拒绝、坐席下线重排队、坐席多会话并发、容量控制、排队超时升级/回退、
邀请超时重试、慢坐席不阻塞、重启恢复（人工/排队/转接/崩溃中间态）、HTTP 端到端。
