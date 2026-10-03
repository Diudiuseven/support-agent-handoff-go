package service

import "errors"

var (
	// ErrConflict 状态冲突（如会话已被接管、重复操作）
	ErrConflict = errors.New("service: state conflict")
	// ErrPermissionDenied 越权操作
	ErrPermissionDenied = errors.New("service: permission denied")
	// ErrInvalidState 当前状态不允许该操作
	ErrInvalidState = errors.New("service: invalid session state")
	// ErrAgentUnavailable 坐席不在线或无容量
	ErrAgentUnavailable = errors.New("service: agent unavailable")
)
