package database

import (
	"context"
	"time"
)

// QuotaWindow is one usage window of a provider quota snapshot. Used and
// Remaining are nil when the probe reported no value; Reset is nil when no
// reset instant was reported.
type QuotaWindow struct {
	Name      string     `json:"name"`
	Used      *float64   `json:"used_percent,omitempty"`
	Remaining *float64   `json:"remaining,omitempty"`
	Limit     float64    `json:"limit_percent"`
	Limited   bool       `json:"limited"`
	Reset     *time.Time `json:"reset_at,omitempty"`
}

// QuotaSnapshot is the neutral probe result: usage windows without any
// admission decision.
type QuotaSnapshot struct {
	Windows []QuotaWindow `json:"windows"`
}

// QuotaState is the neutral durable admission decision for one provider.
// Blocked is nil when the provider is available; Requests counts admitted
// requests since the last check. Windows flattens at the top level of the
// stored JSON, matching the quota service shape.
type QuotaState struct {
	Status   string     `json:"status"`
	Message  string     `json:"message"`
	Checked  time.Time  `json:"checked_at"`
	Next     time.Time  `json:"next_check_at"`
	Blocked  *time.Time `json:"blocked_until,omitempty"`
	Requests int        `json:"requests_since_check"`
	QuotaSnapshot
}

// QuotaStatus is one provider's persisted status label for the settings page.
type QuotaStatus struct {
	Provider string `json:"provider"`
	Status   string `json:"status"`
}

// Quota is the typed port for durable provider quota state. States persist
// as JSON under provider keys capped at 64 KiB on read; oversized or absent
// rows read as the zero state. Writes validate the provider against the
// known provider list and report an unknown-provider error otherwise.
type Quota interface {
	// LoadState returns the persisted admission state for one provider,
	// or the zero state when no row exists or the row is over the size
	// cap. It reports an error for unknown providers or invalid JSON.
	LoadState(ctx context.Context, provider string) (QuotaState, error)
	// SaveState persists the admission state for one provider, stamping
	// the backend clock. It reports an error for unknown providers.
	SaveState(ctx context.Context, provider string, state QuotaState) error
	// LoadStatuses returns the persisted status label per known provider
	// that has a stored state row.
	LoadStatuses(ctx context.Context) ([]QuotaStatus, error)
}
