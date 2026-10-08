package database

import (
	"context"
	"time"
)

// NotificationConfiguration is the resolved webhook delivery selection.
// Target is the stored-encoded webhook URL exactly as persisted; callers
// decode via identity.Decode before delivery or hashing.
type NotificationConfiguration struct {
	Target        string
	Enabled       bool
	NotifyOnError bool
}

// NotificationMessage is one queued outbound message part. Content stays
// encoded exactly as stored; callers decode via identity.Decode.
type NotificationMessage struct {
	ID         string
	EventKey   string
	Position   int
	TargetHash string
	Content    string
}

// NotificationClaim is one claimed delivery. Content stays encoded exactly
// as stored; callers decode via identity.Decode.
type NotificationClaim struct {
	ID       string
	Content  string
	Attempts int
}

// NotificationFailure records a failed delivery attempt. DelaySeconds shifts
// availability forward from the backend clock; Quota preserves the attempt
// budget for rate-limited deliveries.
type NotificationFailure struct {
	ID           string
	Status       string
	Error        string
	DelaySeconds float64
	Quota        bool
}

// NotificationStatusCount aggregates queued messages by status.
type NotificationStatusCount struct {
	Status string
	Count  int64
}

// Notifications is the typed port for durable webhook delivery: admission,
// claiming, acknowledgement, cooldowns, retries and status. Every method on
// a Tx binds to that transaction; cross-feature enqueues commit or roll
// back with the surrounding work.
//
// Locking: claim and retry paths hold the settings mapping locks (same
// extended advisory-lock namespace as Settings mutations) on the caller's
// transaction before reading configuration, so a concurrent destination or
// preference change cannot interleave between the read and the
// destination-hash filter. SQLite serializes on its admitted writer
// transaction instead of advisory locks.
type Notifications interface {
	// LockDeliveryConfig holds the webhook/preferences settings locks on
	// the caller's transaction. It requires a transaction-bound handle.
	LockDeliveryConfig(ctx context.Context) error
	// DeliveryConfig returns the resolved webhook selection under the
	// caller's snapshot, defaulting exactly like the domain reader.
	DeliveryConfig(ctx context.Context) (NotificationConfiguration, error)
	// WebhookTarget returns the stored-encoded webhook URL ("" when none).
	WebhookTarget(ctx context.Context) (string, error)
	// EnqueueMessages admits message parts idempotently: repeats on
	// (event_key, position) are no-ops, never errors.
	EnqueueMessages(ctx context.Context, messages []NotificationMessage) error
	// CancelObsoleteTargets parks pending messages whose destination hash
	// no longer matches the current target, or every pending message when
	// enabled is false. It runs inside the caller's claim transaction
	// while holding the settings locks.
	CancelObsoleteTargets(ctx context.Context, enabled bool, targetHash string) error
	// DestinationPaused reports whether the target hash is inside a
	// recorded cooldown window on the backend clock.
	DestinationPaused(ctx context.Context, targetHash string) (bool, error)
	// ClaimMessage marks the oldest available pending message for the
	// target hash running, increments its attempts and returns it ordered
	// by created_at, event_key, position. It reports ErrNoRows when no
	// message is available.
	ClaimMessage(ctx context.Context, targetHash string) (NotificationClaim, error)
	// AckMessage marks one running message sent. Messages in any other
	// status are left untouched.
	AckMessage(ctx context.Context, id string) error
	// FailMessage requeues (pending) or parks (failed) one running
	// message, recording the error and shifting availability forward by
	// DelaySeconds on the backend clock. Quota deliveries keep their
	// attempt budget; other deliveries keep the incremented claim count.
	FailMessage(ctx context.Context, failure NotificationFailure) error
	// PauseDestination records a cooldown for one target hash until the
	// backend clock plus delay, keeping the farthest known window.
	PauseDestination(ctx context.Context, targetHash string, delay time.Duration) error
	// StatusCounts aggregates queued messages by status.
	StatusCounts(ctx context.Context) ([]NotificationStatusCount, error)
	// LastFailure returns the most recent failed message error ordered by
	// created_at descending. A NULL error reads as "". It reports
	// ErrNoRows when no failed message exists.
	LastFailure(ctx context.Context) (string, error)
	// RetryFailed requeues every failed message for the target hash and
	// returns the requeued count.
	RetryFailed(ctx context.Context, targetHash string) (int64, error)
	// RecoverInterrupted returns every running message to pending (or
	// failed once the retry budget is exhausted), recording the
	// interruption error.
	RecoverInterrupted(ctx context.Context) error
}
