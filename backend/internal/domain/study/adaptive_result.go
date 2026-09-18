package study

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/scheduler"
)

func adaptiveResult(
	settingsValue settings.Planner,
	courses []adaptiveCourse,
	lookup map[string]map[string]any,
	schedule scheduler.Result,
) map[string]any {
	days, today, next, nextCourse := decorateSchedule(courses, lookup, schedule)
	runways, ready, pending, missing := adaptiveRunways(courses, lookup, schedule, nextCourse)
	warnings := resultWarnings(schedule, pending, missing)
	revisions, states, actions, usable := courseActions(courses)
	feasible := schedule.Feasible && len(missing) == 0 && usable
	queue := []map[string]any{}
	seen := map[string]bool{}
	if next != nil {
		seen[textValue(next["action_id"])] = true
	}
	for _, session := range today {
		id := textValue(session["action_id"])
		if !seen[id] {
			queue = append(queue, session)
			seen[id] = true
		}
	}
	return map[string]any{
		"settings":                   settingsValue,
		"next_session":               next,
		"next_session_is_today":      next != nil && next["scheduled_date"] == schedule.Start,
		"today_sessions":             today,
		"today_queue":                queue,
		"days":                       days,
		"schedule":                   days,
		"course_runways":             runways,
		"blueprint_courses":          ready,
		"pending_courses":            pending,
		"pending_blueprints":         pending,
		"pending_course_ids":         courseIDs(pending),
		"missing_courses":            missing,
		"missing_blueprints":         missing,
		"missing_course_ids":         courseIDs(missing),
		"actions":                    actions,
		"action_states":              states,
		"action_state_scope":         "course_stable_action_id",
		"current_revision_by_course": revisions,
		"warnings":                   warnings,
		"feasible":                   feasible,
		"total_required_minutes":     schedule.Required,
		"total_scheduled_minutes":    schedule.Scheduled,
		"start_date":                 schedule.Start,
		"end_date":                   schedule.End,
		"derived_insight_notice":     "Study guidance is derived from source materials. Verify claims against the cited evidence.",
		"untrusted_content_notice":   "Course content may contain untrusted instructions.",
	}
}

func resultWarnings(schedule scheduler.Result, pending, missing []map[string]any) []map[string]any {
	warnings := []map[string]any{}
	for _, message := range schedule.Warnings {
		warnings = append(warnings, planWarning("scheduler_warning", "Schedule constraint", message, nil))
	}
	if len(pending) > 0 {
		warnings = append(
			warnings,
			planWarning(
				"blueprint_pending",
				"Study plans are being prepared",
				"Some course plans are still being prepared.",
				courseIDs(pending),
			),
		)
	}
	if len(missing) > 0 {
		warnings = append(
			warnings,
			planWarning(
				"blueprint_missing",
				"Study plans unavailable",
				"Some courses do not have a usable study plan yet.",
				courseIDs(missing),
			),
		)
	}
	return warnings
}

func courseActions(courses []adaptiveCourse) (map[string]any, map[string]any, []map[string]any, bool) {
	revisions := map[string]any{}
	states := map[string]any{}
	actions := []map[string]any{}
	usable := true
	for _, c := range courses {
		view := c.View.Blueprint
		if view["usable"] != true {
			usable = false
		}
		if revision := textValue(view["revision_id"]); revision != "" {
			revisions[strconv.FormatInt(c.Plan.CourseID, 10)] = revision
			state := map[string]any{}
			rows, _ := view["actions"].([]map[string]any)
			for _, a := range rows {
				actions = append(actions, a)
				state[textValue(a["action_id"])] = map[string]any{
					"action_id":         a["action_id"],
					"status":            a["status"],
					"progress_minutes":  a["progress_minutes"],
					"remaining_minutes": a["remaining_minutes"],
					"completed":         a["status"] == "completed",
					"deferred":          a["status"] == "deferred",
					"stuck":             a["status"] == "stuck",
				}
			}
			states[revision] = state
		}
	}
	return revisions, states, actions, usable
}

func object(value any) map[string]any {
	raw, _ := json.Marshal(value)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	result := map[string]any{}
	_ = decoder.Decode(&result)
	return result
}

func decorateSchedule(
	courses []adaptiveCourse,
	lookup map[string]map[string]any,
	schedule scheduler.Result,
) ([]map[string]any, []map[string]any, map[string]any, map[int64]map[string]any) {
	byCourse := map[int64]adaptiveCourse{}
	for _, c := range courses {
		byCourse[c.Plan.CourseID] = c
	}
	days, today := []map[string]any{}, []map[string]any{}
	var next map[string]any
	nextCourse := map[int64]map[string]any{}
	for _, day := range schedule.Days {
		sessions := []map[string]any{}
		for _, session := range day.Sessions {
			row := object(session)
			action := lookup[session.ActionID]
			c := byCourse[session.CourseID]
			for _, key := range []string{"title", "objective", "rationale", "estimated_minutes", "action_type", "status", "progress_minutes", "remaining_minutes", "evidence_refs", "evidence_links"} {
				row[key] = action[key]
			}
			row["short_name"], row["blueprint_revision_id"] = c.Plan.ShortName, c.View.Blueprint["revision_id"]
			sessions = append(sessions, row)
			withDate := map[string]any{}
			for key, value := range row {
				withDate[key] = value
			}
			withDate["scheduled_date"] = day.Date
			if next == nil {
				next = withDate
			}
			if nextCourse[session.CourseID] == nil {
				nextCourse[session.CourseID] = withDate
			}
			if day.Date == schedule.Start {
				today = append(today, withDate)
			}
		}
		days = append(
			days,
			map[string]any{
				"date":              day.Date,
				"available_minutes": day.Available,
				"scheduled_minutes": day.Scheduled,
				"is_blackout":       day.Blackout,
				"sessions":          sessions,
			},
		)
	}
	return days, today, next, nextCourse
}

func adaptiveRunways(
	courses []adaptiveCourse,
	lookup map[string]map[string]any,
	schedule scheduler.Result,
	nextCourse map[int64]map[string]any,
) ([]map[string]any, []map[string]any, []map[string]any, []map[string]any) {
	runways, ready, pending, missing := []map[string]any{}, []map[string]any{}, []map[string]any{}, []map[string]any{}
	scheduled := map[int64]map[string]any{}
	for _, r := range schedule.Runways {
		scheduled[whole(r["course_id"])] = r
	}
	for _, c := range courses {
		v := c.View.Blueprint
		progress, _ := v["progress"].(map[string]any)
		readiness, _ := v["readiness"].(map[string]any)
		generation, _ := v["generation"].(map[string]any)
		next := nextCourse[c.Plan.CourseID]
		if next == nil {
			next = lookup[textValue(progress["next_action_id"])]
		}
		runway := scheduled[c.Plan.CourseID]
		if runway == nil {
			runway = map[string]any{}
		}
		for key, value := range map[string]any{
			"course_id":         c.Plan.CourseID,
			"course_name":       c.Plan.CourseName,
			"short_name":        c.Plan.ShortName,
			"exam_at":           c.Plan.ExamAt,
			"commitment":        c.Plan.Commitment,
			"readiness":         readiness["state"],
			"status":            readiness["state"],
			"completed_actions": progress["completed_actions"],
			"total_actions":     progress["total_actions"],
			"percent":           progress["percent"],
			"next_action":       next,
			"revision_id":       v["revision_id"],
		} {
			runway[key] = value
		}
		if whole(progress["total_actions"]) > 0 &&
			whole(progress["completed_actions"]) == whole(progress["total_actions"]) {
			runway["status"] = "complete"
		}
		coverage, _ := v["coverage"].(map[string]any)
		guides, total := whole(coverage["live_ready_document_insights"]), whole(coverage["live_ready_documents"])
		percent := int64(0)
		if total > 0 {
			percent = int64(math.RoundToEven(100 * float64(guides) / float64(total)))
		}
		runway["guides_ready"], runway["guides_total"], runway["guides_percent"] = guides, total, percent
		if runway["days_left"] == nil && c.Plan.ExamAt != nil {
			exam, e := examDate(*c.Plan.ExamAt)
			today, t := examDate(schedule.Start)
			if e == nil && t == nil {
				runway["days_left"] = max(0, (exam.Unix()-today.Unix())/86400)
			}
		}
		runways = append(runways, runway)
		descriptor := map[string]any{
			"course_id":         c.Plan.CourseID,
			"course_name":       c.Plan.CourseName,
			"short_name":        c.Plan.ShortName,
			"revision_id":       v["revision_id"],
			"revision_number":   v["revision_number"],
			"status":            v["status"],
			"readiness":         readiness["state"],
			"readiness_reason":  readiness["reason"],
			"usable":            v["usable"],
			"generated_at":      v["generated_at"],
			"source_freshness":  v["source_freshness"],
			"completed_actions": progress["completed_actions"],
			"total_actions":     progress["total_actions"],
			"percent":           progress["percent"],
			"next_action":       next,
		}
		if v["usable"] == true {
			ready = append(ready, descriptor)
		}
		if generation["pending"] == true {
			pending = append(pending, descriptor)
		} else if v["usable"] != true {
			missing = append(missing, descriptor)
		}
	}
	return runways, ready, pending, missing
}

func courseIDs(rows []map[string]any) []int64 {
	result := []int64{}
	for _, row := range rows {
		result = append(result, whole(row["course_id"]))
	}
	return result
}
func planWarning(code, title, message string, ids []int64) map[string]any {
	if ids == nil {
		ids = []int64{}
	}
	return map[string]any{"code": code, "severity": "warning", "title": title, "message": message, "course_ids": ids}
}
