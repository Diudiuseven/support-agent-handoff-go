package domain

import "errors"

// Sentinel errors. Callers use errors.Is to classify failures.
var (
	// ErrSessionNotFound indicates an unknown session id.
	ErrSessionNotFound = errors.New("session not found")
	// ErrSessionClosed indicates an operation on a closed session.
	ErrSessionClosed = errors.New("session is closed")
	// ErrNotQueued indicates the session is not waiting in a queue.
	ErrNotQueued = errors.New("session is not queued")
	// ErrAlreadyAssigned indicates the session already has an owner.
	ErrAlreadyAssigned = errors.New("session is already assigned")
	// ErrNoAgentAvailable means no eligible, online, under-capacity agent.
	ErrNoAgentAvailable = errors.New("no agent available")
	// ErrAgentAtCapacity means the target agent cannot accept more sessions.
	ErrAgentAtCapacity = errors.New("agent is at capacity")
	// ErrAgentOffline indicates the target agent is not online.
	ErrAgentOffline = errors.New("agent is offline")
	// ErrQueueTimeout indicates the queueing deadline elapsed.
	ErrQueueTimeout = errors.New("queue wait timed out")
	// ErrNotTransferring indicates an operation requires an in-flight transfer.
	ErrNotTransferring = errors.New("session is not being transferred")
	// ErrConflict indicates a concurrent optimistic-concurrency conflict.
	ErrConflict = errors.New("concurrent update conflict")
	// ErrUnauthorized indicates the caller lacks permission.
	ErrUnauthorized = errors.New("unauthorized operation")
	// ErrInvalidArgument indicates a malformed request.
	ErrInvalidArgument = errors.New("invalid argument")
)
