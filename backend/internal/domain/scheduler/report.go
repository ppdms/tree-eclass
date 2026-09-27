package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

func feasibilityWarnings(
	actions []*work,
	today time.Time,
	in Input,
	blackouts map[string]bool,
	warnings []string,
) []string {
	seen := map[string]bool{}
	for _, a := range actions {
		if seen[a.Exam] {
			continue
		}
		seen[a.Exam] = true
		var required int64
		for _, peer := range actions {
			if !peer.ExamDate.After(a.ExamDate) {
				required += peer.Remaining
			}
		}
		available := capacity(today, a.ExamDate, in, blackouts)
		if required > available {
			warnings = append(
				warnings,
				fmt.Sprintf(
					"By %s, the plan needs about %dh but only %.1fh are available.",
					a.ExamDate.Format("02 Jan"),
					int64(math.Ceil(float64(required)/60)),
					float64(available)/60,
				),
			)
		}
	}
	return warnings
}

func report(
	result *Result,
	in Input,
	courses map[int64]*course,
	today time.Time,
	blackouts map[string]bool,
	warnings []string,
) {
	ordered, warnings := orderedCourses(result, in, courses, warnings)
	runways(result, in, today, blackouts, ordered)
	nextSession(result)
	result.Warnings = unique(warnings)
}

// orderedCourses collects the plans with a live course, flags infeasible ones
// and returns the courses sorted by exam date.
func orderedCourses(result *Result, in Input, courses map[int64]*course, warnings []string) ([]*course, []string) {
	ordered := []*course{}
	for _, p := range in.Plans {
		c := courses[p.ID]
		if c == nil {
			continue
		}
		ordered = append(ordered, c)
		if c.Remaining == 0 {
			continue
		}
		result.Feasible = false
		warnings = append(
			warnings,
			fmt.Sprintf(
				"%s: %d session(s) remain unscheduled.",
				c.Name,
				int64(math.Ceil(float64(c.Remaining)/float64(in.Session))),
			),
		)
		if c.Commitment == "conditional" {
			warnings = append(
				warnings,
				c.Name+": conditional attempt; it stays below protected courses until you explicitly promote its commitment.",
			)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].Date.Equal(ordered[j].Date) {
			return ordered[i].Date.Before(ordered[j].Date)
		}
		return ordered[i].ID < ordered[j].ID
	})
	return ordered, warnings
}

func runways(result *Result, in Input, today time.Time, blackouts map[string]bool, ordered []*course) {
	for _, c := range ordered {
		percent := int64(100)
		if c.Required > 0 {
			percent = int64(math.RoundToEven(100 * float64(c.Scheduled) / float64(c.Required)))
		}
		result.Runways = append(
			result.Runways,
			map[string]any{
				"course_id":                            c.ID,
				"course_name":                          c.Name,
				"exam_date":                            c.Date.Format(time.DateOnly),
				"exam_at":                              c.Exam,
				"commitment":                           c.Commitment,
				"importance":                           c.Importance,
				"plan_revision":                        c.Revision,
				"days_left":                            (c.Date.Unix() - today.Unix()) / 86400,
				"required_minutes":                     c.Required,
				"scheduled_minutes":                    c.Scheduled,
				"remaining_unscheduled_minutes":        c.Remaining,
				"available_global_minutes_before_exam": capacity(today, c.Date, in, blackouts),
				"schedule_coverage_percent":            percent,
			},
		)
	}
}

func nextSession(result *Result) {
	if len(result.Days) > 0 {
		raw, _ := json.Marshal(result.Days[0].Sessions[0])
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		_ = decoder.Decode(&result.Next)
		result.Next["scheduled_date"] = result.Days[0].Date
	}
}

func unique(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, v := range values {
		if !seen[v] {
			result = append(result, v)
			seen[v] = true
		}
	}
	return result
}
