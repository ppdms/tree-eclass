package settings

import (
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type PlannerErrors []string

func (e PlannerErrors) Error() string { return strings.Join(e, " ") }

func plannerInt(form url.Values, name string, value *int64, low, high int64, issues *PlannerErrors) {
	if !form.Has(name) {
		return
	}
	n, err := strconv.ParseInt(strings.TrimSpace(form.Get(name)), 10, 64)
	if err != nil || n < low || n > high {
		*issues = append(*issues, fmt.Sprintf("%s must be a whole number between %d and %d.", name, low, high))
		return
	}
	*value = n
}

func (p *Planner) apply(form url.Values, issues *PlannerErrors) {
	for i := range 7 {
		key := strconv.Itoa(i)
		n := p.Weekly[key]
		plannerInt(form, "weekly_minutes_"+key, &n, 0, 1440, issues)
		p.Weekly[key] = n
	}
	plannerInt(form, "block_minutes", &p.BlockMinutes, 15, 1440, issues)
	plannerInt(form, "max_courses_per_day", &p.MaxCourses, 1, 7, issues)
	if form.Has("blackout_dates") {
		p.Blackouts = []string{}
		for _, day := range strings.FieldsFunc(form.Get("blackout_dates"), func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) {
			parsed, err := time.Parse(time.DateOnly, day)
			if err != nil || parsed.Format(time.DateOnly) != day {
				*issues = append(*issues, "Blackout dates must use YYYY-MM-DD.")
				continue
			}
			p.Blackouts = append(p.Blackouts, day)
		}
		slices.Sort(p.Blackouts)
		p.Blackouts = slices.Compact(p.Blackouts)
	}
}

func optionalText(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func validExamDate(value string) bool {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04", time.DateOnly} {
		if _, err := time.Parse(layout, strings.Replace(value, " ", "T", 1)); err == nil {
			return true
		}
	}
	return false
}

func (p *ExamPlan) apply(form url.Values, issues *PlannerErrors) {
	suffix := strconv.FormatInt(p.CourseID, 10)
	p.Enabled = form.Has("enabled_" + suffix)
	p.ExamAt = optionalText(form.Get("exam_at_" + suffix))
	if p.Enabled && p.ExamAt == nil {
		*issues = append(*issues, p.CourseName+" needs an exam date when enabled.")
	}
	if p.ExamAt != nil && !validExamDate(*p.ExamAt) {
		*issues = append(*issues, p.CourseName+" has an invalid exam date.")
	}
	if form.Has("commitment_" + suffix) {
		value := strings.TrimSpace(form.Get("commitment_" + suffix))
		if !slices.Contains([]string{"must_pass", "committed", "conditional", "skipped"}, value) {
			*issues = append(*issues, p.CourseName+" has an invalid commitment.")
		} else {
			p.Commitment = value
		}
	}
	if form.Has("target_grade_" + suffix) {
		value, err := strconv.ParseFloat(strings.TrimSpace(form.Get("target_grade_"+suffix)), 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 10 {
			*issues = append(*issues, p.CourseName+" target grade must be between 0 and 10.")
		} else {
			p.TargetGrade = value
		}
	}
	if form.Has("planning_notes_" + suffix) {
		p.Notes = optionalText(form.Get("planning_notes_" + suffix))
		if p.Notes != nil && utf8.RuneCountInString(*p.Notes) > 2000 {
			*issues = append(*issues, p.CourseName+" planning notes must be at most 2000 characters.")
		}
	}
	if form.Has("short_name_" + suffix) {
		p.ShortName = optionalText(form.Get("short_name_" + suffix))
	}
}
