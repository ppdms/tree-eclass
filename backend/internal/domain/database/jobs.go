package database

import "context"

// AppControlCommand is a durable queued action. Timestamps are neutral
// OptionalTime values; Error is nil when no failure was recorded.
type AppControlCommand struct {
	ID          string       `json:"id"`
	Queue       string       `json:"queue"`
	Action      string       `json:"action"`
	Payload     []byte       `json:"payload"`
	Status      string       `json:"status"`
	Attempts    int32        `json:"attempts"`
	AvailableAt OptionalTime `json:"available_at"`
	ClaimedAt   OptionalTime `json:"claimed_at"`
	Error       *string      `json:"error"`
}

// EnqueueCommandParams admits one command. Payload holds JSON bytes.
type EnqueueCommandParams struct {
	ID      string `json:"id"`
	Queue   string `json:"queue"`
	Action  string `json:"action"`
	Payload []byte `json:"payload"`
}

// FailCommandParams requeues or parks a claimed command. DelaySeconds shifts
// availability forward from the backend clock; Error is stored verbatim.
type FailCommandParams struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"`
	Error        *string `json:"error"`
	DelaySeconds int64   `json:"delay_seconds"`
}

// Jobs is the durable work-queue port: admission, claiming and bounded
// retries. QueueLock and PendingCommand must run on the caller's transaction
// so coalescing commits or rolls back with the surrounding work.
type Jobs interface {
	// QueueLock serializes admission/coalescing for one queue key. It is
	// transaction-scoped and requires a transaction-bound handle.
	QueueLock(ctx context.Context, queue string) error
	// PendingCommand returns the oldest pending command id for a queue and
	// action, locking the row on backends with row locks. It reports
	// ErrNoRows when no pending command exists.
	PendingCommand(ctx context.Context, queue, action string) (string, error)
	// EnqueueCommand persists one pending command.
	EnqueueCommand(ctx context.Context, params EnqueueCommandParams) error
	// ClaimCommands marks up to limit available pending commands running,
	// increments their attempts and returns them oldest first.
	ClaimCommands(ctx context.Context, queue string, limit int32) ([]AppControlCommand, error)
	// CompleteCommand deletes one command.
	CompleteCommand(ctx context.Context, id string) error
	// FailCommand requeues (pending) or parks (failed) one claimed command.
	FailCommand(ctx context.Context, params FailCommandParams) error
	// RecoverCommands returns every running command to pending.
	RecoverCommands(ctx context.Context) error
}
