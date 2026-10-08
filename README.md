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
  事务内排序，消息要么在交接前归旧方，要么在交接后归新方。
- **抢占确定**：并发 `Claim` 在全局锁上串行化，第一个满足条件的坐席成功，
  其余得到 `ErrConflict`，结果可解释、可审计。
- **慢坐席不阻塞**：邀请与消息投递均为非阻塞邮箱（缓冲满则保留在持久化层，
  `Delivered=false`），由独立的周期补投循环稳定重投，分发循环永不等待慢坐席。

## 并发下的容量仲裁

坐席容量有两个计数，任何时刻都满足
`Active + Reserved <= Capacity`（绝不超卖，计数不漂移）：

| 计数 | 含义 |
| --- | --- |
| `Active` | 当前由该坐席处理（`agent` 态）的会话数 |
| `Reserved` | 已向该坐席发起转接、等待其接受而**预留**的会话数 |

仲裁规则（所有操作在同一把全局锁内串行、并在单个存储事务内落盘）：

- **发起转接**：目标在线、技能匹配且 `Active+Reserved < Capacity` 才能发起；
  成功瞬间目标 `Reserved++`，等于先锁定一个容量位。
- **接受转接**：目标消耗预留（`Reserved--`、`Active++`），原坐席 `Active--`。
  因为位置在发起时就已预留，即使接受前目标的其他余量被别的会话占满，
  **接受那一刻也一定有位置**，不会再出现"接受失败、会话永久卡在转接中"。
  唯一失败情形是目标坐席此刻已下线（此时转接被确定性终结，见下）。
- **拒绝转接 / 原坐席下线 / 转接超时 / 结束会话**：都先释放目标预留，再做后续
  路由；坐席下线时其 `Active`、`Reserved` 一并清零。
- **抢占邀请（Claim/Offer）**：资格判断统一使用 `Active+Reserved`，
  被预留占住的容量不会再被新邀请/抢占超卖。
- **重启恢复**：以"会话当前归属"与"未完成转接单"为唯一事实来源重算
  `Active`/`Reserved`，修正任何崩溃漂移；不会通过放大声明容量来回避超卖。

## 转接中间态的确定性处理

转接不是"只能等对方接受/拒绝"的悬挂状态，而是**存在时间上界、结果可区分**：

| 结局 | 触发条件 | 路由 | `reason` |
| --- | --- | --- | --- |
| 完成交接 | 目标接受 | 会话切到目标坐席，容量原子切换 | `transferred` |
| 回到原坐席 | 目标拒绝；目标在接受前下线；等待超时，且原坐席仍在线 | 回到原坐席 | `transfer_rejected` / `transfer_target_gone` / `transfer_timeout` |
| 重新排队 | 上述任一情况发生时原坐席已下线/不存在 | 回队列继续分发 | 同上对应原因 |
| 重启收敛 | 进程在转接途中崩溃 | 未完成转接单置失败、释放预留，会话重新排队 | `recovered` |

- 转接单带 `deadline`（`transfer_timeout`，默认 60s）。分发循环每轮先执行
  **转接超时扫描**：到期仍无结果的转接按上表兜底，会话不会长时间停在
  `transferring`，机器人停摆与"只能人工拒绝才能恢复"的问题不再出现。
- 所有终结路径都写 `transfer.finalize` / `transfer.reject` 审计并记录原因，
  回退原坐席还是重新排队在审计与会话 `reason` 中都可区分。

## 积压消息的周期性补投

- 消息落盘与序号推进在同一事务内；坐席邮箱投递在事务外、**非阻塞**。
  邮箱满或坐席暂时不可用时，消息保持 `Delivered=false` 留在持久化层，绝不丢弃。
- 独立的补投循环（`redeliver_interval`，默认 500ms）扫描所有在线坐席名下会话，
  按 `Seq` 顺序逐条补发：只要坐席在线且邮箱有余量，**最终**所有属于它的消息
  都会送达；邮箱满则停在第一条未发出的消息前（保序），下一轮继续，
  不影响其他坐席、其他会话。
- 坐席上线、接管、接受转接、恢复完成时也会立即触发一次补投，周期循环负责兜底，
  不再依赖"恰好发生别的会话操作"。
- 不重：以持久化的 `Delivered` 标记为准，补投与实时投递互斥（同一全局锁）；
  不乱：始终按 `Seq`；不丢：未送达消息始终保留在存储中。
- 可观测：`Service.Stats()` 提供
  `Backlog`（当前未送达消息总数）、`DeliveryFailures`（邮箱满/离线暂缓累计）、
  `Redelivered`（周期补投成功累计）；每次暂缓/失败也有审计事件。

## 排队与容量控制

- 坐席注册时声明 `Capacity`（默认 5），`Active+Reserved >= Capacity` 时
  不再收到邀请、不能再被转接，新会话进入排队。
- 队列按**优先级降序、排队时间升序（FIFO）**分发；坐席上线、容量释放、
  接管/结束都会唤醒分发循环。
- 每个抢占邀请有有效期（`OfferTimeout`），超时未抢占则重新排队并重试
  （`OfferRetries`）；重试耗尽或排队总超时（`QueueTimeout`）时执行既定策略：
  - `escalate`（默认）：升级到高级技能组（`EscalationSkill`，默认 `senior`）、
    优先级 +1，继续排队；已升级过仍无坐席则回退机器人；
  - `fallback`：回退机器人继续处理。
- 处理原因可区分：`handoff_requested`、`claim`、`offer_timeout`、
  `queue_timeout`、`agent_offline`、`transferred`、`transfer_rejected`、
  `transfer_target_gone`、`transfer_timeout`、`escalated`、
  `session_ended`、`recovered`。

## 权限与审计

- `Claim`：坐席/主管角色；坐席技能组须匹配（主管可跨组）；在线且有余量。
- `Transfer`：仅当前处理坐席或主管；目标坐席须在线、有余量、技能匹配。
- `EndSession`：坐席只能结束自己处理的会话。
- 越权操作返回 `ErrPermissionDenied`（HTTP 403）并写入 `allowed=false` 的
  审计事件；所有关键动作（建会话、转人工、邀请、抢占、转接、接受、拒绝、
  上下线、结束、升级、恢复）均有只追加审计记录，可按会话追溯。

## 持久化与重启恢复

- `Store` 接口有两个实现：`MemStore`（纯内存）与 `FileStore`。
- **事务原子性**：`Store.Update(func(tx Store) error)` 在隔离副本上执行一批
  状态变更——会话状态、坐席计数（含 `Reserved`）、交接单、排队条目、审计、
  消息全部在同一事务内：成功则整体替换，失败则整体回滚。`FileStore` 下一次
  事务只对应**一次**快照写盘（临时文件 + fsync + rename + 目录 fsync）。
  因此进程在落盘任意时刻被终止，重启后看到的要么是交接前的完整状态，
  要么是交接后的完整状态，不会出现"会话已归属但无交接单 / 计数不符 /
  审计缺失"之类的半完成中间态。
- 重启后 `Recover` 幂等推进（每个会话一个事务，重复执行不重复处理）：
  - `offering` → 回退排队（邀请已丢失），重新分发；
  - `transferring` → 未完成转接单置失败、释放目标预留，会话重新排队继续推进；
  - `agent` 但交接单 `pending`：
    - 单据指向当前处理方（崩溃在接受落盘途中）→ 补完成交接单；
    - 单据指向他人（崩溃在拒绝/回退途中）→ 以会话当前归属为准，单据对账失败；
    - 缺少交接记录 → 补登一条完成单，保证可追溯；
  - `queued` → 补齐缺失的排队条目，清理残留 pending 单据；
  - 以会话与 pending 单据为事实来源**重算**每个坐席的
    `Active`（名下 agent 会话数）与 `Reserved`（指向它的 pending 转接数），
    修正崩溃漂移（不放大容量）；
  - 在线坐席名下的积压消息立即补投一次，之后由周期补投循环兜底。
- 验证方法：
  - 单测 `go test -race ./...`，重点用例：
    `TestRecoverMidAcceptState`、`TestRecoverMidTransferReleasesReservation`、
    `TestRecoverMissingHandoffRecord`、`internal/store/tx_test.go`（事务回滚/提交）；
  - 手工：用 `FileStore`（设置 `HANDOFF_DATA_DIR`）跑到转接/接受等中间状态后
    `kill -9` 进程，再启动；检查 `/sessions/{id}` 状态、`/agents` 的
    `active/reserved`、`/sessions/{id}/handoffs` 与 `/audit`，
    应满足：无 `transferring` 残留（排队中除外）、`active+reserved<=capacity`、
    每个 agent 归属都有对应完成交接单。

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
| `transfer_timeout` | `HANDOFF_TRANSFER_TIMEOUT` | `60s` | 转接等待接受的最长时间，超时确定性兜底 |
| `redeliver_interval` | `HANDOFF_REDELIVER_INTERVAL` | `500ms` | 积压消息周期性补投扫描间隔 |

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
| GET | `/stats` | 投递可观测指标（backlog/delivery_failures/redelivered） |

## 运行与测试

```bash
go build ./...
go test -race ./...
go run ./cmd/server   # 默认 :8080，纯内存；设置 HANDOFF_DATA_DIR 持久化
```

测试覆盖：交接全流程与消息顺序、多坐席并发抢占（唯一胜者）、越权接管拒绝、
转接接受/拒绝、坐席下线重排队、坐席多会话并发、容量控制、排队超时升级/回退、
邀请超时重试、慢坐席不阻塞、重启恢复（人工/排队/转接/崩溃中间态）、HTTP 端到端。

新增的并发/可靠性用例（均可 `go test -race -count=N ./...` 反复稳定运行）：

- `TestTransferAcceptAfterCapacityFilled`：发起转接后目标余量被占满，接受仍确定成功；
- `TestConcurrentTransferCapacityArbitration`：并发发起/接受转接与抢占交织，
  验证不超卖、无预留泄漏、无会话卡在 `transferring`；
- `TestTransferTargetOfflineResolves` / `TestTransferTimeoutResolves`：
  目标下线与等待超时的确定性兜底及可区分原因；
- `TestRecoverMidAcceptState` / `TestRecoverMidTransferReleasesReservation` /
  `TestRecoverMissingHandoffRecord`：交接落盘途中崩溃后的完整状态恢复与幂等；
- `TestSlowAgentBacklogAutoRedeliver` / `TestRedeliverIsolationAcrossAgents`：
  慢坐席积压消息按序、不重、不丢自动补投，且不影响其他坐席；
- `internal/store/tx_test.go`：事务成功整体生效、失败整体回滚。

## 兼容性

所有对外接口（HTTP 路由与请求/响应字段）、坐席注册/调用方式与既有配置项保持
兼容：新增配置项均有默认值，旧配置无需迁移；新增的 `Agent.reserved`、
`Handoff.deadline` 字段为 JSON 附加字段，旧快照可直接加载。
