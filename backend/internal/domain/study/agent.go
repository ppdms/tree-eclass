package study

import (
	"context"
	"slices"
	"time"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Priorities schedules arbitrary MCP course subsets from already-published
// navigation content. It never rebuilds source evidence or invokes a provider.
// Both source admission and the pure scheduler share a read-only snapshot.
func (s Service) Priorities(ctx context.Context, requested []int64, limit int, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id FROM app.courses WHERE hidden=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range requested {
		if !slices.Contains(ids, id) {
			return nil, knowledge.ErrUnavailable
		}
	}
	if len(requested) > 0 {
		ids = slices.Clone(requested)
	}
	if ids == nil {
		ids = []int64{}
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	adaptive, err := s.buildAdaptiveSet(ctx, tx, ids, studyDay(now))
	if err != nil {
		return nil, err
	}
	intelligence, err := buildIntelligenceSet(ctx, tx, nil, ids, a, studyDay(now))
	if err != nil {
		return nil, err
	}
	result := agentPriorities(adaptive, intelligence, min(20, max(1, limit)))
	return result, tx.Commit(ctx)
}

func selectFields(row map[string]any, keys ...string) map[string]any {
	result := map[string]any{}
	for _, key := range keys {
		if value, exists := row[key]; exists {
			result[key] = value
		}
	}
	return result
}
func selectRows(rows []map[string]any, keys ...string) []map[string]any {
	result := []map[string]any{}
	for _, row := range rows {
		result = append(result, selectFields(row, keys...))
	}
	return result
}
func agentPriorities(adaptive, intelligence map[string]any, limit int) map[string]any {
	result := map[string]any{
		"derived_insight_notice":   knowledge.DerivedNotice,
		"untrusted_content_notice": knowledge.UntrustedNotice,
		"coverage":                 intelligence["coverage"],
	}
	for _, key := range []string{"pending_courses", "pending_blueprints", "missing_courses", "missing_blueprints", "warnings"} {
		result[key] = adaptive[key]
	}
	runways, _ := adaptive["course_runways"].([]map[string]any)
	compactRunways := selectRows(
		runways,
		"course_id",
		"course_name",
		"short_name",
		"exam_at",
		"days_left",
		"commitment",
		"readiness",
		"percent",
		"required_minutes",
		"scheduled_minutes",
		"remaining_unscheduled_minutes",
		"schedule_coverage_percent",
	)
	compact := selectFields(
		adaptive,
		"start_date",
		"end_date",
		"feasible",
		"total_required_minutes",
		"total_scheduled_minutes",
		"warnings",
	)
	compact["course_runways"] = compactRunways
	result["adaptive_plan"] = compact
	ready, _ := adaptive["blueprint_courses"].([]map[string]any)
	if len(ready) > 0 {
		result["planning_mode"] = "adaptive_blueprint"
		today, _ := adaptive["today_sessions"].([]map[string]any)
		result["today_sessions"] = selectRows(
			today,
			"minutes",
			"kind",
			"unit_title",
			"unit_objective",
			"instruction",
			"success_criterion",
			"course_name",
			"days_to_exam",
			"evidence_links",
		)
		var total int64
		for _, session := range today {
			total += whole(session["minutes"])
		}
		result["today_total_minutes"], result["next_session"], result["course_runways"] = total, adaptive["next_session"], compactRunways
		result["blueprint_courses"] = selectRows(
			ready,
			"course_id",
			"course_name",
			"short_name",
			"status",
			"readiness",
			"readiness_reason",
			"usable",
			"percent",
			"total_actions",
			"completed_actions",
			"revision_number",
		)
		return result
	}
	result["planning_mode"], result["provisional"], result["limit_applied"] = "provisional_file_priorities", true, limit
	materials, _ := intelligence["focus_queue"].([]*PriorityMaterial)
	queue := []map[string]any{}
	for _, material := range materials[:min(limit, len(materials))] {
		queue = append(queue, agentMaterial(material))
	}
	result["focus_queue"], result["exam_collisions"] = queue, intelligence["exam_collisions"]
	runways, _ = intelligence["exam_runways"].([]map[string]any)
	for _, runway := range runways {
		materials, _ := runway["next_materials"].([]*PriorityMaterial)
		rows := []map[string]any{}
		for _, m := range materials {
			rows = append(rows, agentMaterial(m))
		}
		runway["next_materials"], runway["readiness_percent"] = rows, runway["readiness"]
	}
	result["exam_runways"] = runways
	return result
}
func agentMaterial(m *PriorityMaterial) map[string]any {
	evidence := "official_material"
	if m.Origin == "external" {
		evidence = "course_material"
	}
	return map[string]any{
		"document_id":                 m.ID,
		"course_id":                   m.CourseID,
		"course_name":                 m.CourseName,
		"display_name":                m.Name,
		"source_path":                 m.Path,
		"source_origin":               m.Origin,
		"evidence_class":              evidence,
		"document_kind":               m.Kind,
		"page_count":                  m.Pages,
		"word_count":                  m.Words,
		"reading_minutes":             m.Reading,
		"complexity_score":            m.Complexity,
		"study_level":                 m.Level,
		"priority":                    m.Priority,
		"days_left":                   m.Days,
		"study_insight":               m.AI,
		"resource_uri":                "eclass://documents/" + m.ID,
		"derived_not_source_evidence": true,
		"untrusted_content":           true,
	}
}
