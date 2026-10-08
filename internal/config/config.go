package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
)

// Config 服务配置项（可由 JSON 文件或 HANDOFF_* 环境变量覆盖）
type Config struct {
	DataDir          string        `json:"data_dir"`          // 持久化目录，空表示纯内存
	QueueTimeout     time.Duration `json:"queue_timeout"`     // 排队总超时，超时后按 DefaultPolicy 处理
	OfferTimeout     time.Duration `json:"offer_timeout"`     // 抢占邀请有效期
	OfferRetries     int           `json:"offer_retries"`     // 邀请无人响应时的重试次数
	TransferTimeout  time.Duration `json:"transfer_timeout"`  // 转接请求有效期；超时未接受由系统自动裁决
	DefaultPolicy    model.Policy  `json:"default_policy"`    // 超时/坐席不可用策略：escalate | fallback
	DispatchInterval time.Duration `json:"dispatch_interval"` // 队列分发扫描间隔
	MailboxSize      int           `json:"mailbox_size"`      // 坐席邮箱缓冲大小
	DefaultCapacity  int           `json:"default_capacity"`  // 坐席默认并发容量
	EscalationSkill  string        `json:"escalation_skill"`  // 升级目标技能组
	ServerAddr       string        `json:"server_addr"`       // HTTP 监听地址
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		DataDir:          "",
		QueueTimeout:     5 * time.Minute,
		OfferTimeout:     30 * time.Second,
		OfferRetries:     2,
		TransferTimeout:  30 * time.Second,
		DefaultPolicy:    model.PolicyEscalate,
		DispatchInterval: 200 * time.Millisecond,
		MailboxSize:      256,
		DefaultCapacity:  5,
		EscalationSkill:  "senior",
		ServerAddr:       ":8080",
	}
}

// Load 从 JSON 文件加载，文件不存在时返回默认配置；环境变量可覆盖
func Load(path string) (Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	applyEnv(&cfg)
	return cfg, nil
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("HANDOFF_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("HANDOFF_QUEUE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.QueueTimeout = d
		}
	}
	if v := os.Getenv("HANDOFF_OFFER_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.OfferTimeout = d
		}
	}
	if v := os.Getenv("HANDOFF_OFFER_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.OfferRetries = n
		}
	}
	if v := os.Getenv("HANDOFF_TRANSFER_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.TransferTimeout = d
		}
	}
	if v := os.Getenv("HANDOFF_DEFAULT_POLICY"); v != "" {
		cfg.DefaultPolicy = model.Policy(v)
	}
	if v := os.Getenv("HANDOFF_DISPATCH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.DispatchInterval = d
		}
	}
	if v := os.Getenv("HANDOFF_MAILBOX_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MailboxSize = n
		}
	}
	if v := os.Getenv("HANDOFF_DEFAULT_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DefaultCapacity = n
		}
	}
	if v := os.Getenv("HANDOFF_ESCALATION_SKILL"); v != "" {
		cfg.EscalationSkill = v
	}
	if v := os.Getenv("HANDOFF_SERVER_ADDR"); v != "" {
		cfg.ServerAddr = v
	}
}
