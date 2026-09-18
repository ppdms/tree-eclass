// Package quota owns cached, reset-aware model admission and sanitized state.
package quota

import (
	"time"
)

type Window struct {
	Name      string     `json:"name"`
	Used      *float64   `json:"used_percent,omitempty"`
	Remaining *float64   `json:"remaining,omitempty"`
	Limit     float64    `json:"limit_percent"`
	Limited   bool       `json:"limited"`
	Reset     *time.Time `json:"reset_at,omitempty"`
}
type Snapshot struct {
	Windows []Window `json:"windows"`
}
type State struct {
	Status   string     `json:"status"`
	Message  string     `json:"message"`
	Checked  time.Time  `json:"checked_at"`
	Next     time.Time  `json:"next_check_at"`
	Blocked  *time.Time `json:"blocked_until,omitempty"`
	Requests int        `json:"requests_since_check"`
	Snapshot
}

func evaluate(snapshot Snapshot, now time.Time) State {
	state := State{
		Status:   "available",
		Message:  "Provider usage is below the configured thresholds.",
		Checked:  now,
		Next:     now.Add(time.Minute),
		Snapshot: snapshot,
	}
	var resets []time.Time
	exceeded := 0
	for _, w := range snapshot.Windows {
		if !w.Limited && (w.Used == nil || *w.Used < w.Limit) && (w.Remaining == nil || *w.Remaining > 0) {
			continue
		}
		exceeded++
		if w.Reset != nil && w.Reset.After(now) {
			resets = append(resets, *w.Reset)
		}
	}
	if exceeded == 0 {
		return state
	}
	until := now.Add(time.Minute)
	if len(resets) == exceeded {
		for _, reset := range resets {
			if reset.After(until) {
				until = reset
			}
		}
		until = until.Add(30 * time.Second)
	}
	state.Status, state.Message, state.Next, state.Blocked = "paused", "Provider usage reached its threshold; waiting for a fresh quota check.", until, &until
	return state
}
func paused(status, message string, now time.Time, delay time.Duration) State {
	until := now.Add(delay)
	return State{Status: status, Message: message, Checked: now, Next: until, Blocked: &until}
}
