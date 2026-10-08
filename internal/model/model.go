package model

import "time"

// SessionID 会话唯一标识
type SessionID string

// AgentID 坐席唯一标识
type AgentID string

// SessionState 会话状态机状态
type SessionState string

const (
	StateBot          SessionState = "bot"          // 机器人处理中
	StateQueued       SessionState = "queued"       // 排队等待人工
	StateOffering     SessionState = "offering"     // 已向坐席发起抢占邀请
	StateAgent        SessionState = "agent"        // 人工坐席处理中
	StateTransferring SessionState = "transferring" // 坐席转接中
	StateEnded        SessionState = "ended"        // 会话已结束
)

// Policy 排队超时/坐席不可用时的处理策略
type Policy string

const (
	PolicyEscalate Policy = "escalate" // 升级（提升优先级/转高级技能组）
	PolicyFallback Policy = "fallback" // 回退机器人
)

// Reason 可区分的处理原因
type Reason string

const (
	ReasonHandoffRequested   Reason = "handoff_requested"
	ReasonQueueTimeout       Reason = "queue_timeout"
	ReasonOfferTimeout       Reason = "offer_timeout"
	ReasonAgentOffline       Reason = "agent_offline"
	ReasonAgentUnavailable   Reason = "agent_unavailable"
	ReasonTransferred        Reason = "transferred"
	ReasonTransferRejected   Reason = "transfer_rejected"
	ReasonTransferTargetGone Reason = "transfer_target_gone"
	ReasonTransferTimeout    Reason = "transfer_timeout"
	ReasonSessionEnded       Reason = "session_ended"
	ReasonRecovered          Reason = "recovered"
	ReasonEscalated          Reason = "escalated"
	ReasonClaim              Reason = "claim"
)

// Role 操作者角色（用于权限校验）
type Role string

const (
	RoleBot        Role = "bot"
	RoleAgent      Role = "agent"
	RoleSupervisor Role = "supervisor"
	RoleSystem     Role = "system"
)

// Session 会话聚合根
type Session struct {
	ID          SessionID    `json:"id"`
	State       SessionState `json:"state"`
	HandlerType string       `json:"handler_type"`
	HandlerID   string       `json:"handler_id"`
	Skill       string       `json:"skill"`
	Priority    int          `json:"priority"`
	Seq         int64        `json:"seq"`
	OfferID     string       `json:"offer_id"`
	Escalated   bool         `json:"escalated"`
	Reason      Reason       `json:"reason"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	EndedAt     *time.Time   `json:"ended_at"`
}

// Agent 坐席
type Agent struct {
	ID       AgentID  `json:"id"`
	Name     string   `json:"name"`
	Role     Role     `json:"role"`
	Skills   []string `json:"skills"`
	Capacity int      `json:"capacity"`
	Online   bool     `json:"online"`
	Active   int      `json:"active"`
	// Reserved 已预留给该坐席、但尚未完成接受的转接会话数。
	// 容量仲裁不变量：Active + Reserved <= Capacity 恒成立。
	Reserved  int       `json:"reserved"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Message 会话消息（按 Seq 严格有序）
type Message struct {
	ID          string    `json:"id"`
	SessionID   SessionID `json:"session_id"`
	Seq         int64     `json:"seq"`
	From        string    `json:"from"`
	HandlerType string    `json:"handler_type"`
	HandlerID   string    `json:"handler_id"`
	Body        string    `json:"body"`
	Delivered   bool      `json:"delivered"`
	CreatedAt   time.Time `json:"created_at"`
}

// HandoffStatus 交接记录状态
type HandoffStatus string

const (
	HandoffPending   HandoffStatus = "pending"
	HandoffCompleted HandoffStatus = "completed"
	HandoffFailed    HandoffStatus = "failed"
)

// Handoff 交接记录（机器人<->坐席、坐席<->坐席），用于追溯
type Handoff struct {
	ID          string        `json:"id"`
	SessionID   SessionID     `json:"session_id"`
	FromType    string        `json:"from_type"`
	FromID      string        `json:"from_id"`
	ToType      string        `json:"to_type"`
	ToID        string        `json:"to_id"`
	Reason      Reason        `json:"reason"`
	Status      HandoffStatus `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	// Deadline 仅转接类 pending 交接单使用：超过该时间仍未完成则由后台
	// 确定性兜底（回退原坐席或重新排队），保证会话不会长期停在转接中间态。
	Deadline *time.Time `json:"deadline,omitempty"`
}

// AuditEvent 全流程审计事件（只追加）
type AuditEvent struct {
	ID        string    `json:"id"`
	SessionID SessionID `json:"session_id"`
	ActorID   string    `json:"actor_id"`
	ActorRole Role      `json:"actor_role"`
	Action    string    `json:"action"`
	Allowed   bool      `json:"allowed"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// QueueEntry 排队条目
type QueueEntry struct {
	SessionID     SessionID `json:"session_id"`
	Skill         string    `json:"skill"`
	Priority      int       `json:"priority"`
	OfferID       string    `json:"offer_id"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
	QueueDeadline time.Time `json:"queue_deadline"`
	OfferDeadline time.Time `json:"offer_deadline"`
	OfferCount    int       `json:"offer_count"`
}

// EventType 投递给坐席的事件类型
type EventType string

const (
	EventMessage EventType = "message"
	EventOffer   EventType = "offer"
	EventNotice  EventType = "notice"
)

// Event 坐席邮箱中的事件
type Event struct {
	Type      EventType `json:"type"`
	SessionID SessionID `json:"session_id"`
	Message   *Message  `json:"message,omitempty"`
	Text      string    `json:"text,omitempty"`
	At        time.Time `json:"at"`
}
