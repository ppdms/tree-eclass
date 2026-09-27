package study

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/scheduler"
	"tree-eclass/internal/domain/settings"
)

type adaptiveCourse struct {
	Plan settings.ExamPlan
	View navigation.View
}

func (s Service) buildAdaptive(
	ctx context.Context,
	tx pgx.Tx,
	selected *int64,
	today time.Time,
) (map[string]any, error) {
	var included []int64
	if selected != nil {
		included = []int64{*selected}
	}
	return s.buildAdaptiveSet(ctx, tx, included, today)
}

func (s Service) buildAdaptiveSet(
	ctx context.Context,
	tx pgx.Tx,
	included []int64,
	today time.Time,
) (map[string]any, error) {
	settingsValue, err := settings.ReadPlanner(ctx, tx)
	if err != nil {
		return nil, err
	}
	plans, err := settings.ReadExamPlans(ctx, tx)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(plans, func(i, j int) bool {
		a, b := "9999", "9999"
		if plans[i].ExamAt != nil {
			a = *plans[i].ExamAt
		}
		if plans[j].ExamAt != nil {
			b = *plans[j].ExamAt
		}
		if a != b {
			return a < b
		}
		return plans[i].CourseID < plans[j].CourseID
	})
	in := scheduler.Input{
		Start:      today.Format(time.DateOnly),
		Session:    settingsValue.BlockMinutes,
		Weekly:     settingsValue.Weekly,
		Blackouts:  settingsValue.Blackouts,
		MaxCourses: int(settingsValue.MaxCourses),
		Progress:   map[string]int64{},
	}
	lookup := map[string]map[string]any{}
	courses, err := s.adaptiveCourses(ctx, tx, plans, included, &in, lookup)
	if err != nil {
		return nil, err
	}
	schedule, err := scheduler.Build(ctx, in)
	if err != nil {
		return nil, err
	}
	return adaptiveResult(settingsValue, courses, lookup, schedule), nil
}

func (s Service) adaptiveCourses(
	ctx context.Context,
	tx pgx.Tx,
	plans []settings.ExamPlan,
	included []int64,
	in *scheduler.Input,
	lookup map[string]map[string]any,
) ([]adaptiveCourse, error) {
	courses := []adaptiveCourse{}
	sourceBytes := 0
	for _, plan := range plans {
		if !plan.Enabled || plan.Commitment == "skipped" ||
			included != nil && !slices.Contains(included, plan.CourseID) {
			continue
		}
		view, err := (navigation.Service{Pool: s.Pool}).ReadTx(
			ctx,
			tx,
			navigation.Request{CourseID: plan.CourseID, Roadmap: true, IncludeHidden: true, IncludeActions: true},
		)
		if err != nil {
			return nil, err
		}
		sourceBytes += view.SourceBytes
		if sourceBytes > 16*1024*1024 {
			return nil, errors.New("study projection exceeds 16 MiB of source plan content")
		}
		courses = append(courses, adaptiveCourse{plan, view})
		if view.Blueprint["usable"] != true {
			continue
		}
		in.Plans = append(in.Plans, plannerCourse(plan, view.Blueprint, in, lookup))
	}
	return courses, nil
}

var actionKinds = map[string]string{
	"read":       "learn",
	"recall":     "recall",
	"solve":      "practice",
	"write":      "practice",
	"draw":       "practice",
	"implement":  "practice",
	"compare":    "practice",
	"review":     "review",
	"diagnostic": "practice",
}
var examValues = map[string]float64{"critical": .95, "high": .8, "medium": .55, "low": .3}

func plannerCourse(
	p settings.ExamPlan,
	view map[string]any,
	in *scheduler.Input,
	lookup map[string]map[string]any,
) scheduler.Plan {
	result := scheduler.Plan{
		ID:         p.CourseID,
		Name:       p.CourseName,
		Commitment: p.Commitment,
		Importance: 1,
		Revision:   textValue(view["revision_id"]),
		Units:      []scheduler.Unit{},
	}
	if p.ExamAt != nil {
		result.Exam = *p.ExamAt
	}
	byUnit := map[string][]map[string]any{}
	actions, _ := view["actions"].([]map[string]any)
	for _, action := range actions {
		id := textValue(action["action_id"])
		lookup[id] = action
		key := textValue(action["unit_key"])
		byUnit[key] = append(byUnit[key], action)
		switch action["status"] {
		case "completed":
			in.Completed = append(in.Completed, id)
		case "deferred":
			in.Deferred = append(in.Deferred, id)
		case "stuck":
			in.Stuck = append(in.Stuck, id)
		}
		in.Progress[id] = whole(action["progress_minutes"])
	}
	blueprint, _ := view["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	for i, raw := range units {
		unit, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		priority := textValue(unit["priority"])
		u := scheduler.Unit{
			Key:           textValue(unit["key"]),
			Title:         textValue(unit["title"]),
			Objective:     textValue(unit["objective"]),
			Priority:      priority,
			Order:         i + 1,
			Value:         examValues[priority],
			Prerequisites: stringsValue(unit["depends_on"]),
			Evidence:      stringsValue(unit["evidence_refs"]),
		}
		for _, action := range byUnit[u.Key] {
			kind := textValue(action["action_type"])
			u.Actions = append(
				u.Actions,
				scheduler.Action{
					ID:            textValue(action["action_id"]),
					Kind:          actionKinds[kind],
					BlueprintKind: kind,
					Instruction:   textValue(action["instruction"]),
					Success:       textValue(action["success_criterion"]),
					Minutes:       whole(action["estimated_minutes"]),
					Evidence:      stringsValue(action["evidence_refs"]),
				},
			)
		}
		result.Units = append(result.Units, u)
	}
	return result
}

func textValue(value any) string { text, _ := value.(string); return text }
func whole(value any) int64 {
	switch n := value.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		result, _ := n.Int64()
		return result
	}
	return 0
}
func stringsValue(value any) []string {
	result := []string{}
	if rows, ok := value.([]any); ok {
		for _, row := range rows {
			if text, ok := row.(string); ok {
				result = append(result, text)
			}
		}
	}
	return result
}
