package scheduler

import (
	"strconv"
	"time"
)

func allocation(remaining, session, capacity int64) int64 {
	allocated := min(remaining, session, capacity)
	left := remaining - allocated
	if left <= 0 || left >= 15 {
		return allocated
	}
	trimmed := remaining - 15
	if trimmed >= 15 && trimmed <= capacity {
		return trimmed
	}
	if remaining <= capacity {
		return remaining
	}
	return 0
}

func availability(day time.Time, in Input, blackouts map[string]bool) int64 {
	if blackouts[day.Format(time.DateOnly)] {
		return 0
	}
	weekday := (int(day.Weekday()) + 6) % 7
	return max(0, in.Weekly[strconv.Itoa(weekday)])
}

// Capacity sums whole weeks arithmetically, so distant exam dates do not add
// repeated day-by-day work to every action comparison.
func capacity(start, end time.Time, in Input, blackouts map[string]bool) int64 {
	days := int((end.Unix() - start.Unix()) / 86400)
	if days <= 0 {
		return 0
	}
	var week, total int64
	for i := range 7 {
		week += max(0, in.Weekly[strconv.Itoa(i)])
	}
	total = int64(days/7) * week
	for i := range days % 7 {
		day := start.AddDate(0, 0, i)
		weekday := (int(day.Weekday()) + 6) % 7
		total += max(0, in.Weekly[strconv.Itoa(weekday)])
	}
	for key := range blackouts {
		day, err := date(key)
		if err == nil && !day.Before(start) && day.Before(end) {
			weekday := (int(day.Weekday()) + 6) % 7
			total -= max(0, in.Weekly[strconv.Itoa(weekday)])
		}
	}
	return total
}

func pick(
	actions []*work,
	courses map[int64]*course,
	day, today time.Time,
	in Input,
	blackouts, deferred, stuck map[string]bool,
	done map[int64]map[string]bool,
	todayCourses map[int64]bool,
	unusable map[int]bool,
) int {
	first := unitOrder(actions)
	available := courseAvailability(actions, day, in, blackouts)
	selected := -1
	var best [4]float64
	for i, a := range actions {
		if a.Remaining <= 0 || !day.Before(a.ExamDate) || unusable[i] {
			continue
		}
		if day.Equal(today) && (deferred[a.ActionID] || stuck[a.ActionID]) {
			continue
		}
		if !eligible(a, done, first, todayCourses, in) {
			continue
		}
		slack := available[a.CourseID] - courses[a.CourseID].Remaining
		density := (.35 + a.Value) / float64(max(in.Session, a.Remaining))
		contextFactor := 1.0
		if len(todayCourses) > 0 && !todayCourses[a.CourseID] {
			contextFactor = .72
		}
		score := commitments[a.Commitment] * a.Importance * weights[a.Kind] * (1 + 50*density) * contextFactor
		rank := [4]float64{-float64(slack), score, -float64(a.Order), -float64(a.ActionOrder)}
		if selected < 0 || greater(rank, best) {
			selected, best = i, rank
		}
	}
	return selected
}

type unitRef struct {
	Course int64
	Key    string
}

func unitOrder(actions []*work) map[unitRef]int {
	first := map[unitRef]int{}
	for _, a := range actions {
		if a.Remaining <= 0 {
			continue
		}
		key := unitRef{a.CourseID, a.UnitKey}
		if prior, ok := first[key]; !ok || a.ActionOrder < prior {
			first[key] = a.ActionOrder
		}
	}
	return first
}

func courseAvailability(actions []*work, day time.Time, in Input, blackouts map[string]bool) map[int64]int64 {
	available := map[int64]int64{}
	for _, a := range actions {
		if a.Remaining <= 0 {
			continue
		}
		if _, ok := available[a.CourseID]; !ok {
			available[a.CourseID] = max(in.Session, capacity(day, a.ExamDate, in, blackouts))
		}
	}
	return available
}

// eligible rejects actions whose prerequisites are unmet, that are not the
// first action of their unit, or that would exceed the per-day course cap.
func eligible(
	a *work,
	done map[int64]map[string]bool,
	first map[unitRef]int,
	todayCourses map[int64]bool,
	in Input,
) bool {
	for _, key := range a.Prerequisites {
		if !done[a.CourseID][key] {
			return false
		}
	}
	if first[unitRef{a.CourseID, a.UnitKey}] < a.ActionOrder {
		return false
	}
	return todayCourses[a.CourseID] || len(todayCourses) < in.MaxCourses
}

func greater(a, b [4]float64) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return false
}
