package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/audit"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// Service 客服会话交接与排队分发服务。
// 所有状态变更在同一把全局锁 mu 内串行执行，因此：
//   - 会话在任一时刻只有一个处理方（状态机 + CAS 保证）
//   - 交接/接管/转接的多条记录（会话、交接单、审计、消息）原子落盘
//   - 并发抢占结果确定（先到先得，后者得到明确冲突）
//
// 消息投递到坐席邮箱在锁外进行（非阻塞），慢坐席不会阻塞其他会话。
type Service struct {
	cfg config.Config
	st  store.Store
	log *audit.Auditor

	mu       sync.Mutex
	stopCh   chan struct{}
	wg       sync.WaitGroup
	wake     chan struct{}
	stopOnce sync.Once

	mbMu      sync.Mutex
	mailboxes map[model.AgentID]chan *model.Event

	// 投递可观测计数（在 s.mu 临界区内更新）
	redeliveryAttempts int64 // 周期补投累计尝试投递的消息数
	deliveryFailures   int64 // 因邮箱满等原因未能送达的次数（含初次投递与补投）

	// tx 非 nil 表示当前正处在一个业务事务内：
	// 所有 store 读写经 db() 路由到事务工作副本；通知延迟到提交成功后发出，
	// 保证"对方收到的事件"与"已落盘的状态"一致，不会看到不存在的转接。
	tx *txn

	now func() time.Time
}

// txn 一次业务操作的事务上下文
type txn struct {
	store.Tx
	notify    []notifyItem  // offer/notice：Commit 后投递
	delivered []deliveredEv // message：已在事务内预占邮箱槽位
}

type notifyItem struct {
	agentID model.AgentID
	ev      *model.Event
}

type deliveredEv struct {
	agentID model.AgentID
	ev      *model.Event
}

func New(cfg config.Config, st store.Store) *Service {
	if cfg.DispatchInterval <= 0 {
		cfg.DispatchInterval = 200 * time.Millisecond
	}
	if cfg.MailboxSize <= 0 {
		cfg.MailboxSize = 256
	}
	if cfg.DefaultCapacity <= 0 {
		cfg.DefaultCapacity = 5
	}
	if cfg.OfferRetries <= 0 {
		cfg.OfferRetries = 2
	}
	if cfg.TransferTimeout <= 0 {
		cfg.TransferTimeout = 30 * time.Second
	}
	return &Service{
		cfg:       cfg,
		st:        st,
		log:       audit.New(st),
		stopCh:    make(chan struct{}),
		wake:      make(chan struct{}, 1),
		mailboxes: make(map[model.AgentID]chan *model.Event),
		now:       time.Now,
	}
}

// Run 启动后台分发循环并执行重启恢复
func (s *Service) Run(ctx context.Context) error {
	if err := s.st.Init(ctx); err != nil {
		return fmt.Errorf("store init: %w", err)
	}
	if err := s.Recover(ctx); err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	s.wg.Add(1)
	go s.dispatchLoop(ctx)
	return nil
}

// Stop 停止后台循环（幂等）
func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
		_ = s.st.Close()
	})
}

func (s *Service) dispatchLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.DispatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.safeDispatch(ctx)
		case <-s.wake:
			s.safeDispatch(ctx)
		}
	}
}

func (s *Service) safeDispatch(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.inTx(ctx, func() error {
		// 1) 排队分发（含转接超时裁决）
		if err := s.dispatchLocked(ctx); err != nil {
			return err
		}
		// 2) 周期性补投所有在线坐席名下的积压消息
		return s.redeliverLocked(ctx)
	}); err != nil {
		// 分发错误不应终止循环；记录审计便于排查
		_ = s.st.AppendAudit(ctx, audit.Event("", "system", model.RoleSystem, "dispatch.error", false, err.Error()))
	}
}

// kickDispatch 唤醒分发循环（非阻塞）
func (s *Service) kickDispatch() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// ---- 以下辅助方法均需在持有 s.mu 的情况下调用 ----

// db 返回当前应使用的存储：事务内为工作副本，事务外为正式存储。
func (s *Service) db(ctx context.Context) store.Store {
	if s.tx != nil {
		return s.tx
	}
	return s.st
}

// commitErr 包装一个"变更应当提交、但同时要返回给调用方"的错误
// （典型场景：接受转接时容量已被占满，系统已确定性地回退/重排队，
// 变更必须落盘，但 AcceptTransfer 仍需返回 ErrAgentUnavailable）。
type commitErr struct{ err error }

func (e *commitErr) Error() string { return e.err.Error() }

// commitWithError 在事务内标记：提交已发生的变更，并把 cause 返回给调用方。
func (s *Service) commitWithError(cause error) error {
	return &commitErr{err: cause}
}

// inTx 在单个事务内执行 fn：
//   - fn 返回 nil：Commit，事务内的全部状态变更整体生效，随后才发送延迟通知；
//   - fn 返回 *commitErr：Commit（保留变更），并把内部错误返回给调用方；
//   - fn 返回其他错误：Rollback，状态回到操作前，错误原样返回。
//
// 调用方必须已持有 s.mu（事务内不释放锁，保证事务串行）。
func (s *Service) inTx(ctx context.Context, fn func() error) error {
	if s.tx != nil {
		return fn() // 已在事务内（如 Recover 的恢复过程复用同一事务）
	}
	t, err := s.st.BeginTx(ctx)
	if err != nil {
		return err
	}
	s.tx = &txn{Tx: t}
	committed := false
	var returnErr error
	defer func() {
		if !committed {
			// 回滚：退回事务内预占的消息邮箱槽位
			for i := len(s.tx.delivered) - 1; i >= 0; i-- {
				d := s.tx.delivered[i]
				ch := s.mailboxFor(d.agentID)
				select {
				case <-ch:
				default:
				}
			}
			t.Rollback()
		}
		s.tx = nil
	}()
	fnErr := fn()
	var ce *commitErr
	if errors.As(fnErr, &ce) {
		returnErr = ce.err
	} else if fnErr != nil {
		return fnErr
	}
	if err := t.Commit(); err != nil {
		return err
	}
	committed = true
	// 消息事件已在 fn 内（持锁）入箱并随事务提交；这里投递 offer/notice。
	for _, n := range s.tx.notify {
		s.enqueueLocked(n.agentID, n.ev)
	}
	return returnErr
}

// enqueueLocked 在事务内真实占用一个邮箱槽位（调用方持 s.mu，
// 所有邮箱写点也都在 s.mu 内，因此与容量探测之间不存在竞态）。
// 邮箱满时返回 false，不改变任何状态——调用方应保持消息 Delivered=false，
// 交由周期补投送达。
func (s *Service) enqueueLocked(agentID model.AgentID, ev *model.Event) bool {
	ch := s.mailboxFor(agentID)
	select {
	case ch <- ev:
		return true
	default:
		return false
	}
}

func (s *Service) auditLocked(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role, action string, allowed bool, detail string) {
	e := audit.Event(sessionID, actorID, role, action, allowed, detail)
	if e.CreatedAt.IsZero() {
		e.CreatedAt = s.now()
	}
	if e.ID == "" {
		e.ID = newID("audit")
	}
	// allowed=false 的审计表示"被拒绝的操作"：业务变更会随事务回滚，
	// 但拒绝事实必须独立保留、可追溯，因此直接写正式存储而非事务副本。
	if !allowed {
		_ = s.st.AppendAudit(ctx, e)
		return
	}
	_ = s.db(ctx).AppendAudit(ctx, e)
}

func (s *Service) mailboxFor(agentID model.AgentID) chan *model.Event {
	s.mbMu.Lock()
	defer s.mbMu.Unlock()
	ch, ok := s.mailboxes[agentID]
	if !ok {
		ch = make(chan *model.Event, s.cfg.MailboxSize)
		s.mailboxes[agentID] = ch
	}
	return ch
}

// notifyAgent 安排一个非关键事件（offer/notice）的投递。
// 事务内登记，Commit 成功后才入箱（坐席不会看到未生效的转接/邀请）；
// 事务外直接投递。非阻塞：邮箱满即丢弃该通知（关键状态可通过拉取接口获得）。
func (s *Service) notifyAgent(agentID model.AgentID, ev *model.Event) {
	if s.tx != nil {
		s.tx.notify = append(s.tx.notify, notifyItem{agentID: agentID, ev: ev})
		return
	}
	s.enqueueLocked(agentID, ev)
}

// deliverMessageLocked 在事务内投递一条消息事件（调用方持 s.mu）：
// 立即真实占用邮箱槽位，成功则返回 true（由调用方把 Delivered=true 在同一事务落盘）；
// 邮箱满则返回 false，消息保持 Delivered=false 并计数，由周期补投送达。
// "占槽"与"标记落盘"在同一把 s.mu 临界区、随同一事务提交，入箱顺序与持久化
// 顺序一致，正常运行期不重、不丢、不乱序。
func (s *Service) deliverMessageLocked(agentID model.AgentID, ev *model.Event) bool {
	if s.enqueueLocked(agentID, ev) {
		if s.tx != nil {
			s.tx.delivered = append(s.tx.delivered, deliveredEv{agentID: agentID, ev: ev})
		}
		return true
	}
	s.deliveryFailures++
	return false
}

// deliverMailbox 非阻塞写入坐席邮箱（事务外使用）
func (s *Service) deliverMailbox(agentID model.AgentID, ev *model.Event) {
	s.enqueueLocked(agentID, ev)
}

// eligibleAgents 返回当前可承接该会话的在线坐席：
// 在线、有余量、技能匹配（主管可跨技能组）
func (s *Service) eligibleAgents(ctx context.Context, skill string) ([]*model.Agent, error) {
	agents, err := s.db(ctx).ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	var out []*model.Agent
	for _, a := range agents {
		if !a.Online || a.Active >= a.Capacity {
			continue
		}
		if a.Role == model.RoleSupervisor {
			out = append(out, a)
			continue
		}
		for _, sk := range a.Skills {
			if sk == skill {
				out = append(out, a)
				break
			}
		}
	}
	return out, nil
}

// sortedQueue 按优先级降序、排队时间升序（FIFO）返回排队条目
func sortedQueue(entries []*model.QueueEntry) []*model.QueueEntry {
	out := make([]*model.QueueEntry, len(entries))
	copy(out, entries)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].EnqueuedAt.Before(out[j].EnqueuedAt)
	})
	return out
}

func newID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
