package service

import (
	"context"
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

	now func() time.Time
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
	if err := s.dispatchLocked(ctx); err != nil {
		// 分发错误不应终止循环；记录审计便于排查
		_ = s.log.Record(ctx, audit.Event("", "system", model.RoleSystem, "dispatch.error", false, err.Error()))
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

func (s *Service) auditLocked(ctx context.Context, sessionID model.SessionID, actorID string, role model.Role, action string, allowed bool, detail string) {
	_ = s.log.Record(ctx, audit.Event(sessionID, actorID, role, action, allowed, detail))
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

// notifyAgent 非阻塞投递；邮箱满时不等待（慢坐席不阻塞分发），
// 事件仍可通过 ListOffers/ListMessages 拉取，不丢。
func (s *Service) notifyAgent(agentID model.AgentID, ev *model.Event) {
	ch := s.mailboxFor(agentID)
	select {
	case ch <- ev:
	default:
	}
}

// eligibleAgents 返回当前可承接该会话的在线坐席：
// 在线、有余量、技能匹配（主管可跨技能组）
func (s *Service) eligibleAgents(ctx context.Context, skill string) ([]*model.Agent, error) {
	agents, err := s.st.ListAgents(ctx)
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
