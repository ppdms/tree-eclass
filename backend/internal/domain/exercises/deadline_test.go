package exercises

import (
	"testing"
	"time"
)

func TestGreekDeadlines(t *testing.T) {
	now := time.Date(2026, 3, 28, 20, 0, 0, 0, athens)
	for _, test := range []struct{ raw, want string }{
		{"Τετάρτη, 25 Μαρτίου 2026 - 11:55 μ.μ.", "2026-03-25T23:55:00+02:00"},
		{"αύριο - 12:15 π.μ.", "2026-03-29T00:15:00+02:00"},
		{"μεθαύριο - 12:00 μ.μ.", "2026-03-30T12:00:00+03:00"},
		{"31 Φεβρουαρίου 2026 - 11:55 μ.μ.", ""},
		{"25 Μαρτίου 2026 - 11:99 μ.μ.", ""},
		{"no deadline", ""},
	} {
		got := Deadline(test.raw, now)
		if test.want == "" {
			if !got.IsZero() {
				t.Errorf("accepted invalid deadline %q", test.raw)
			}
			continue
		}
		if got.Format(time.RFC3339) != test.want {
			t.Errorf("%s: %s", test.raw, got)
		}
	}
	for _, test := range []struct {
		delta     time.Duration
		submitted bool
		want      string
	}{
		{-time.Minute, false, "ex-overdue"},
		{0, false, "ex-critical"},
		{24 * time.Hour, false, "ex-warning"},
		{72 * time.Hour, false, "ex-moderate"},
		{168 * time.Hour, false, "ex-ok"},
		{-time.Minute, true, "ex-submitted"},
	} {
		got, _ := Urgency(now.Add(test.delta), test.submitted, now)
		if got != test.want {
			t.Errorf("%v submitted=%v: %s", test.delta, test.submitted, got)
		}
	}
}
