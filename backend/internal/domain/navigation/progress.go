package navigation

import (
	"encoding/json"
	"math"
	"slices"

	"tree-eclass/internal/domain/identity"
)

type actionRow struct {
	Payload map[string]any `json:"payload"`
	Status  string         `json:"status"`
	Minutes int64          `json:"progress_minutes"`
}
type unitStats struct {
	Key      string `json:"unit_key"`
	Total    int64  `json:"total_actions"`
	Complete int64  `json:"completed_actions"`
}

func actionPayload(row actionRow) map[string]any {
	value, _ := identity.DecodeJSON(row.Payload).(map[string]any)
	if value == nil {
		value = map[string]any{}
	}
	estimated := integer(value["estimated_minutes"])
	progress := row.Minutes
	if row.Status == "completed" {
		progress = estimated
	}
	value["status"], value["progress_minutes"], value["remaining_minutes"] = row.Status, progress, max(
		0,
		estimated-min(progress, estimated),
	)
	return value
}

func integer(value any) int64 {
	switch n := value.(type) {
	case json.Number:
		v, _ := n.Int64()
		return v
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}
func percent(complete, total int64) int64 {
	if total == 0 {
		return 0
	}
	return int64(math.RoundToEven(100 * float64(complete) / float64(total)))
}

func decorateProgress(view map[string]any, rawUnits, rawNext, rawActions []byte, request Request) error {
	var stats []unitStats
	if err := decodeJSON(rawUnits, &stats); err != nil {
		return err
	}
	var next any
	if len(rawNext) > 0 {
		var row actionRow
		if err := decodeJSON(rawNext, &row); err != nil {
			return err
		}
		next = actionPayload(row)
	}
	var total, complete int64
	byKey := map[string]unitStats{}
	for _, unit := range stats {
		total += unit.Total
		complete += unit.Complete
		byKey[unit.Key] = unit
	}
	progress := map[string]any{
		"total_actions":     total,
		"completed_actions": complete,
		"percent":           percent(complete, total),
		"next_action":       next,
	}
	view["progress"] = progress
	blueprint, _ := view["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	for _, item := range units {
		unit, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key, _ := unit["key"].(string)
		stat := byKey[key]
		unit["total_actions"], unit["completed_actions"], unit["progress_percent"] = stat.Total, stat.Complete, percent(
			stat.Complete,
			stat.Total,
		)
	}
	if !request.Roadmap {
		return nil
	}
	var actions []actionRow
	if err := decodeJSON(rawActions, &actions); err != nil {
		return err
	}
	values := []map[string]any{}
	for _, row := range actions {
		values = append(values, actionPayload(row))
	}
	view["actions"] = values
	progress["next_action_id"] = nil
	if action, ok := next.(map[string]any); ok {
		progress["next_action_id"] = action["action_id"]
	}
	delete(progress, "next_action")
	if !request.IncludeActions {
		summarizeRoadmap(view, blueprint, units)
	}
	return nil
}

func summarizeRoadmap(view, blueprint map[string]any, units []any) {
	delete(view, "actions")
	delete(view, "evidence_links")
	view["actions_deferred"] = true
	if blueprint == nil {
		return
	}
	support := false
	for _, key := range []string{"question_families", "conflicts", "coverage_gaps"} {
		if items, ok := blueprint[key].([]any); ok && len(items) > 0 {
			support = true
		}
	}
	strategy, ok := blueprint["exam_strategy"].(map[string]any)
	blueprint["deferred_sections"] = map[string]bool{"strategy": ok && len(strategy) > 0, "support": support}
	for _, key := range []string{"exam_strategy", "question_families", "conflicts", "coverage_gaps"} {
		delete(blueprint, key)
	}
	for _, item := range units {
		unit, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for key := range unit {
			if !slices.Contains(
				[]string{
					"key",
					"title",
					"priority",
					"estimated_minutes",
					"total_actions",
					"completed_actions",
					"progress_percent",
				},
				key,
			) {
				delete(unit, key)
			}
		}
	}
}
