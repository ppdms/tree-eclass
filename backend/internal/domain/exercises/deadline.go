// Package exercises owns assignment reads and deadline presentation.
package exercises

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

var athens = func() *time.Location { loc, _ := time.LoadLocation("Europe/Athens"); return loc }()
var relativeDeadline = regexp.MustCompile(`(?i)(σήμερα|αύριο|μεθαύριο)\s*-\s*(\d+):(\d+)\s*(μ\.μ\.|π\.μ\.)`)
var absoluteDeadline = regexp.MustCompile(`(\d+)\s+(\S+)\s+(\d{4})\s*-\s*(\d+):(\d+)\s*(μ\.μ\.|π\.μ\.)`)

var months = map[string]time.Month{
	"Ιανουαρίου":  1,
	"Φεβρουαρίου": 2,
	"Μαρτίου":     3,
	"Απριλίου":    4,
	"Μαΐου":       5,
	"Ιουνίου":     6,
	"Ιουλίου":     7,
	"Αυγούστου":   8,
	"Σεπτεμβρίου": 9,
	"Οκτωβρίου":   10,
	"Νοεμβρίου":   11,
	"Δεκεμβρίου":  12,
}

var urgencyOrder = map[string]int{
	"ex-overdue":   0,
	"ex-critical":  1,
	"ex-warning":   2,
	"ex-moderate":  3,
	"ex-ok":        4,
	"ex-unknown":   5,
	"ex-submitted": 6,
}

func number(s string) int { n, _ := strconv.Atoi(s); return n }

func Deadline(raw string, now time.Time) time.Time {
	raw = strings.Join(strings.Fields(raw), " ")
	if m := relativeDeadline.FindStringSubmatch(raw); m != nil {
		days := map[string]int{"σήμερα": 0, "αύριο": 1, "μεθαύριο": 2}[strings.ToLower(m[1])]
		base := now.In(athens).AddDate(0, 0, days)
		return clock(base.Year(), base.Month(), base.Day(), number(m[2]), number(m[3]), strings.ToLower(m[4]))
	}
	if m := absoluteDeadline.FindStringSubmatch(raw); m != nil {
		return clock(number(m[3]), months[m[2]], number(m[1]), number(m[4]), number(m[5]), m[6])
	}
	return time.Time{}
}

func clock(year int, month time.Month, day, hour, minute int, period string) time.Time {
	if month < 1 || month > 12 || day < 1 || day > 31 || hour < 1 || hour > 12 || minute < 0 || minute > 59 {
		return time.Time{}
	}
	hour %= 12
	if period == "μ.μ." {
		hour += 12
	}
	result := time.Date(year, month, day, hour, minute, 0, 0, athens)
	if result.Day() != day || result.Month() != month {
		return time.Time{}
	}
	return result
}

func Urgency(deadline time.Time, submitted bool, now time.Time) (string, *string) {
	if submitted {
		return "ex-submitted", nil
	}
	if deadline.IsZero() {
		return "ex-unknown", nil
	}
	seconds := int64(deadline.Sub(now).Seconds())
	label, urgency := "Overdue", "ex-overdue"
	days, hours, minutes := seconds/86400, seconds%86400/3600, seconds%3600/60
	switch {
	case deadline.Before(now):
	case seconds < 86400:
		urgency, label = "ex-critical", fmt.Sprintf("%dh %dm left", hours, minutes)
	case seconds < 3*86400:
		urgency, label = "ex-warning", fmt.Sprintf("%dd %dh left", days, hours)
	case seconds < 7*86400:
		urgency, label = "ex-moderate", fmt.Sprintf("%dd %dh left", days, hours)
	default:
		urgency, label = "ex-ok", fmt.Sprintf("%dd left", days)
	}
	return urgency, &label
}
