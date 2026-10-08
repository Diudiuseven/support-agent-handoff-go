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
- **转接确定**：转接不预留目标容量；接受时在同一临界区做最终容量仲裁，
  余量已被占满则确定性地回退原坐席/重新排队，并带可区分原因；
  转接单有 `Deadline`，超时由系统自动裁决，会话不会停在中间态。
- **慢坐席不阻塞**：邀请与消息投递均为非阻塞邮箱（缓冲满则保留在持久化层，
  由周期性补投循环按序补投，也可通过 `ListOffers/ListMessages` 拉取），
  分发循环永不等待慢坐席。

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
  `transfer_target_full`、`transfer_timeout`、`escalated`、
  `session_ended`、`recovered`。

### 并发下的容量仲裁规则

所有状态变更都在同一把全局锁 + 单个存储事务内串行执行，因此：

- **检查与占用同一临界区**：`Claim` / 接受转接时，`Active >= Capacity`
  的判断与 `Active++` 之间不存在让出锁的窗口，接受成功的那一刻目标坐席
  必然真的有一个空位。
- **转接不预留容量**：发起转接只做提示性校验，不预先占用目标坐席名额；
  真正的占用发生在"接受"时。这样"发起后、接受前余量被别的会话占满"是
  合法时序，由接受时的最终仲裁处理。
- **容量切换原子化**：接受转接时"原坐席 `Active--` + 目标坐席 `Active++`
  + 会话归属切换 + 交接单完成 + 审计"在同一事务内整体生效。
- **拒绝/超时/下线交织**：拒绝转接回退原坐席（计数不变）；原坐席已下线
  则释放原坐席占用并重新排队。重启时一律按会话真实归属重算 `Active`，
  只修正计数、**绝不抬高 `Capacity`**。
- 任何坐席任一时刻 `Active <= Capacity` 是全局不变量，
  可通过 `GET /stats` 的 `active_by_agent` / `capacity_by_agent` 核对。

### 转接中间态的确定性处理

会话处于 `transferring` 时，原坐席仍是唯一处理方（消息仍归原方、仍占原方
容量）。转接在任意并发时序下都有且只有一个确定结果：

| 情形 | 结果 | 会话 reason |
| --- | --- | --- |
| 目标接受且仍有余量 | 原子切换到目标坐席 | `transferred` |
| 目标拒绝 | 回退原坐席（原在线）/ 重新排队（原离线） | `transfer_rejected` |
| 接受时余量已被占满或目标离线 | 回退原坐席 / 重新排队，`Accept` 返回 `ErrAgentUnavailable` | `transfer_target_full` |
| 到达转接单 `Deadline` 仍未决 | 分发循环自动裁决：回退原坐席 / 重新排队 | `transfer_timeout` |
| 转接期间原坐席下线 | 转接单失败，立即重新排队 | `agent_offline` |

回退原坐席时容量不变化；重新排队时释放原坐席占用并生成到 `queue` 的完成
交接单。所有结果都有失败/完成交接单与审计记录，可按会话追溯。

## 权限与审计

- `Claim`：坐席/主管角色；坐席技能组须匹配（主管可跨组）；在线且有余量。
- `Transfer`：仅当前处理坐席或主管；目标坐席须在线、有余量、技能匹配。
- `EndSession`：坐席只能结束自己处理的会话。
- 越权操作返回 `ErrPermissionDenied`（HTTP 403）并写入 `allowed=false` 的
  审计事件；所有关键动作（建会话、转人工、邀请、抢占、转接、接受、拒绝、
  上下线、结束、升级、恢复）均有只追加审计记录，可按会话追溯。

## 消息积压与周期性补投

- 每条消息在全局锁内分配单调递增 `Seq`，归属当前处理方。
- 坐席邮箱（缓冲 `mailbox_size`）非阻塞写入：**入箱与 `Delivered=true`
  在同一临界区、同一事务内完成**，因此正常运行期消息不重复、不丢失、不乱序。
- 邮箱满（慢坐席/不在线）时消息保留 `Delivered=false` 于持久化层。
- **补投循环**：分发循环每 `dispatch_interval` 扫描所有在线坐席名下
  `agent`/`transferring` 状态的会话，把未送达消息**按 `Seq` 升序**补投；
  某个会话一旦遇到邮箱已满立即停止该会话（不跳过、不跳号），下周期继续，
  因此最终全部送达且会话内严格有序；慢坐席不会阻塞其他坐席/会话。
- 坐席上线、接管、接受转接、重启恢复时也会立即触发一次补投。
- 跨进程崩溃边界：邮箱是内存态，崩溃后未消费的事件会因消息仍为
  `Delivered=false` 而在重启后再投一次（at-least-once）；消费端可用
  `session_id + seq` 去重。正常运行期为 exactly-once。

### 投递可观测性（`GET /stats`）

| 字段 | 含义 |
| --- | --- |
| `pending` | 全局未送达消息总数 |
| `pending_by_agent` | 每个坐席名下未送达消息数（慢坐席积压） |
| `active_by_agent` / `capacity_by_agent` | 坐席当前处理数 / 声明容量 |
| `redelivery_attempts` | 周期补投累计成功投递的消息数 |
| `delivery_failures` | 因邮箱满等原因未能立即送达的累计次数 |
| `queued` / `transferring` | 当前排队 / 转接中的会话数 |

## 持久化、事务与重启恢复

- `Store` 接口有两个实现：`MemStore`（纯内存）与 `FileStore`。
- **事务原子性**：`BeginTx` 返回在状态深拷贝上工作的事务，`Commit` 时整体
  替换内存状态，FileStore 额外只做**一次**原子快照
  （临时文件 + fsync + rename + 目录 fsync）。每次业务操作（会话、坐席
  计数、交接单、排队条目、消息标记、审计的联动修改）都在单个事务内完成，
  因此进程在任意时刻被终止，重启后看到的要么是操作前的完整状态，要么是
  操作后的完整状态，不会出现"会话已切换但交接单缺失/计数不符/审计缺失"
  的半完成态。被拒绝操作的 `allowed=false` 审计独立落盘，不随业务回滚丢失。
- 重启后 `Recover` 在**单个事务**内幂等推进：
  - `offering` → 回退排队（邀请已丢失），重新分发；
  - `transferring` → 未完成转接单失败，回退在线原坐席或重新排队
    （与超时裁决同一套逻辑，原因 `recovered`）；
  - `agent` 但交接单 `pending` → 补完成交接单（状态切换与单据落盘之间
    崩溃）；缺少交接记录则补登，保证可追溯；
  - `queued` → 补齐排队条目，重新计时；
  - 按会话真实归属重算所有坐席 `Active`（修正漂移，不抬高 `Capacity`）；
  - 在线坐席名下的积压消息保持未送达，由补投循环继续按序推送。
- 排队中与未完成的交接在重启后会继续推进；恢复幂等，重复执行不产生重复
  单据、重复排队条目或重复计数，全过程有审计记录。

### 重启恢复后的验证方法

- `GET /sessions`：确认不存在 `transferring` 状态；`agent` 态会话的
  `handler_id` 非空。
- `GET /agents` 或 `GET /stats`：每个坐席 `active == 其名下 agent 态会话数
  <= capacity`。
- `GET /sessions/{id}/handoffs`：无残留 `pending` 单；每个 agent 态会话都有
  指向当前处理方的 `completed` 单。
- `GET /sessions/{id}/messages`：`seq` 从 1 连续无重复；恢复后继续发消息
  序号连续。
- `GET /queue`：排队条目与 `queued` 态会话一一对应。
- 自动化验证见
  `internal/api/crash_recovery_test.go`（SIGKILL 强杀真实进程后重启校验
  上述全部不变量）与 `internal/service/*_test.go`。

## 配置项

| 字段 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `data_dir` | `HANDOFF_DATA_DIR` | 空（内存） | 持久化目录；非空时使用 FileStore |
| `queue_timeout` | `HANDOFF_QUEUE_TIMEOUT` | `5m` | 排队总超时 |
| `offer_timeout` | `HANDOFF_OFFER_TIMEOUT` | `30s` | 抢占邀请有效期 |
| `offer_retries` | `HANDOFF_OFFER_RETRIES` | `2` | 邀请无响应重试次数 |
| `transfer_timeout` | `HANDOFF_TRANSFER_TIMEOUT` | `30s` | 转接请求有效期；超时未接受由系统自动裁决 |
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
| GET | `/stats` | 容量/积压/投递计数等可观测状态 |

## 运行与测试

```bash
go build ./...
go test -race ./...
go run ./cmd/server   # 默认 :8080，纯内存；设置 HANDOFF_DATA_DIR 持久化
```

测试覆盖：交接全流程与消息顺序、多坐席并发抢占（唯一胜者）、越权接管拒绝、
转接接受/拒绝、**转接发起与目标余量被占满并发（30 轮）、并发接受与容量
竞争仲裁、转接超时自动裁决（回退/重排队）**、坐席下线重排队、坐席多会话
并发、容量控制、排队超时升级/回退、邀请超时重试、慢坐席不阻塞、
**慢坐席积压消息周期性补投（最终送达/不重/不乱序/多会话顺序）、坐席重连
后继续补投**、Store 事务提交/回滚原子性（含 `Delivered` 持久化）、
重启恢复（人工/排队/转接/半完成交接单/原坐席离线）、
**SIGKILL 真实进程崩溃后重启的全局不变量校验**、HTTP 端到端与 `/stats`。

可反复运行：

```bash
go test -race -count=10 ./...          # 并发场景多轮压测
go test ./internal/api/ -run Crash     # 真实进程崩溃恢复（会构建 server 二进制）
```
