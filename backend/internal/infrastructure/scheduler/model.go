// Package scheduler allocates validated study actions without model calls.
package scheduler

import "time"

type Plan struct {
	ID         int64   `json:"course_id"`
	Name       string  `json:"course_name"`
	Exam       string  `json:"exam_at"`
	Commitment string  `json:"commitment"`
	Importance float64 `json:"importance"`
	Revision   string  `json:"plan_revision"`
	Units      []Unit  `json:"units"`
}
type Unit struct {
	Key           string   `json:"key"`
	Title         string   `json:"title"`
	Objective     string   `json:"objective"`
	Priority      string   `json:"priority"`
	Order         int      `json:"order"`
	Value         float64  `json:"exam_value"`
	Prerequisites []string `json:"prerequisite_keys"`
	Evidence      []string `json:"evidence_refs"`
	Actions       []Action `json:"actions"`
}
type Action struct {
	ID            string   `json:"id"`
	Kind          string   `json:"kind"`
	BlueprintKind string   `json:"blueprint_kind"`
	Instruction   string   `json:"instruction"`
	Success       string   `json:"success_criterion"`
	Minutes       int64    `json:"estimated_minutes"`
	Evidence      []string `json:"evidence_refs"`
}
type Input struct {
	Plans      []Plan           `json:"plans"`
	Weekly     map[string]int64 `json:"weekly_minutes"`
	Blackouts  []string         `json:"blackout_dates"`
	Session    int64            `json:"session_minutes"`
	MaxCourses int              `json:"max_courses_per_day"`
	Completed  []string         `json:"completed_action_ids"`
	Deferred   []string         `json:"deferred_action_ids"`
	Stuck      []string         `json:"stuck_action_ids"`
	Progress   map[string]int64 `json:"action_progress_minutes"`
	Start      string           `json:"start_date"`
}
type Session struct {
	ActionID      string   `json:"action_id"`
	Revision      string   `json:"plan_revision"`
	CourseID      int64    `json:"course_id"`
	CourseName    string   `json:"course_name"`
	UnitKey       string   `json:"unit_key"`
	UnitTitle     string   `json:"unit_title"`
	UnitObjective string   `json:"unit_objective"`
	UnitPriority  string   `json:"unit_priority"`
	Kind          string   `json:"kind"`
	BlueprintKind string   `json:"blueprint_kind"`
	Instruction   string   `json:"instruction"`
	Success       string   `json:"success_criterion"`
	Minutes       int64    `json:"minutes"`
	Exam          string   `json:"exam_date"`
	Days          int64    `json:"days_to_exam"`
	Evidence      []string `json:"evidence_refs"`
}
type Day struct {
	Date      string    `json:"date"`
	Available int64     `json:"available_minutes"`
	Scheduled int64     `json:"scheduled_minutes"`
	Sessions  []Session `json:"sessions"`
	Blackout  bool      `json:"is_blackout"`
}
type Result struct {
	Start     string           `json:"start_date"`
	End       string           `json:"end_date"`
	Days      []Day            `json:"days"`
	Next      map[string]any   `json:"next_session"`
	Runways   []map[string]any `json:"course_runways"`
	Warnings  []string         `json:"warnings"`
	Feasible  bool             `json:"feasible"`
	Required  int64            `json:"total_required_minutes"`
	Scheduled int64            `json:"total_scheduled_minutes"`
}
type work struct {
	Session
	ExamDate           time.Time
	Commitment         string
	Importance, Value  float64
	Order, ActionOrder int
	Prerequisites      []string
	Remaining          int64
}
type course struct {
	Plan
	Date                           time.Time
	Required, Remaining, Scheduled int64
}

var commitments = map[string]float64{"must_pass": 1.55, "committed": 1.25, "conditional": 0.72, "skipped": 0}
var weights = map[string]float64{"timed_mock": 1.3, "practice": 1.2, "recall": 1.08, "review": 1, "learn": 0.95}

func date(value string) (time.Time, error) {
	if len(value) > 10 {
		value = value[:10]
	}
	return time.Parse(time.DateOnly, value)
}

func set(values []string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		result[value] = true
	}
	return result
}
