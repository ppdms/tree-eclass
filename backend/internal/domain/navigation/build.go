package navigation

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
)

// Refresh publishes at most one dirty course per tick. Model calls and document
// extraction are separate jobs; this processor only reads local, saved evidence.
func (s Service) Refresh(ctx context.Context) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return false, err
	}
	target, err := tx.Navigation().StaleTarget(ctx, a.AnalysisGeneration())
	if errors.Is(err, database.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, selected, err := courses.SnapshotCourses(ctx, tx, &target.CourseID)
	if err != nil {
		return false, err
	}
	view, err := buildView(ctx, tx, *selected, a)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return s.Publish(ctx, target.CourseID, target.Generation, a, view)
}

type revision struct {
	ID, Number, Attempts         int64
	Hash, Status, Model, Created string
	Generated                    *string
}

func history(ctx context.Context, ops database.Operations, course int64, a settings.AI) ([]revision, error) {
	rows, err := ops.Navigation().BlueprintHistory(ctx, course, settings.CourseAnalysisVersion, a.CourseModel)
	if err != nil {
		return nil, err
	}
	result := []revision{}
	for _, row := range rows {
		result = append(result, revision{
			ID: row.ID, Number: row.Number, Hash: row.Hash, Status: row.Status,
			Model: row.Model, Attempts: row.Attempts, Created: row.Created, Generated: row.Generated,
		})
	}
	return result, nil
}

func buildView(ctx context.Context, tx database.Tx, c courses.Course, a settings.AI) (map[string]any, error) {
	rows, err := history(ctx, tx, c.ID, a)
	if err != nil {
		return nil, err
	}
	view := emptyView(c, a, rows)
	plan, err := settings.ReadExamPlan(ctx, tx, c.ID)
	if err != nil {
		return nil, err
	}
	view["current_plan"] = plan
	view["practice"] = map[string]any{
		"enabled":              a.PracticeEnabled && a.CourseEnabled && a.EnrichmentEnabled,
		"pending_units":        0,
		"question_count":       0,
		"units_with_questions": 0,
		"set_counts":           map[string]int64{},
	}
	readiness, err := knowledge.Readiness(ctx, tx, c.ID, a)
	if err != nil {
		return nil, err
	}
	view["readiness"].(map[string]any)["source_readiness"] = readiness
	defer func() { view["readiness"].(map[string]any)["source_readiness"] = readiness }()
	var ready *revision
	for i := range rows {
		if rows[i].Status == "ready" {
			ready = &rows[i]
			break
		}
	}
	if ready == nil {
		return view, nil
	}
	validated, links, packet, reason, err := readyBlueprint(ctx, tx, c, a, ready)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		return invalidate(view, reason), nil
	}
	decorated, err := blueprints.Decorate(validated, c.ID, c.Name, ready.Hash, links)
	if err != nil {
		return nil, err
	}
	view["usable"], view["revision_id"] = true, ready.Hash
	view["revision_hash"], view["revision_number"] = ready.Hash, ready.Number
	view["blueprint"], view["evidence_links"] = decorated, links
	view["model"], view["generated_at"] = ready.Model, ready.Generated
	state, reason := "ready", "A validated blueprint is ready."
	if rows[0].Number > ready.Number && a.CourseEnabled && a.EnrichmentEnabled {
		if rows[0].Status == "pending" || rows[0].Status == "running" {
			state, reason = "refreshing", "A newer blueprint is being generated."
		}
		if rows[0].Status == "failed" {
			state, reason = "refresh_failed", "The last usable blueprint remains active because its refresh failed."
		}
	}
	view["readiness"] = map[string]any{
		"state":           state,
		"reason":          reason,
		"usable":          true,
		"refresh_pending": state == "refreshing",
	}
	view["generation"].(map[string]any)["usable_revision_id"] = ready.Hash
	unitKeys := make([]string, 0, len(validated.Units))
	for _, unit := range validated.Units {
		unitKeys = append(unitKeys, unit.Key)
	}
	view["practice"], err = knowledge.PracticeSummary(ctx, tx, c.ID, ready.Hash, unitKeys, a)
	if err != nil {
		return nil, err
	}
	progress, _ := packet["course_progress"].(map[string]any)
	view["coverage"] = map[string]any{
		"fully_covered": progress["coverage_state"] == "complete" && readiness["settled"] == true &&
			integer(progress["documents_with_ready_insight"]) == integer(readiness["ready_document_insights"]),
		"plan_snapshot":                progress,
		"live_ready_documents":         readiness["ready_documents"],
		"live_ready_document_insights": readiness["ready_document_insights"],
		"live_settled":                 readiness["settled"],
	}
	return view, nil
}

func readyBlueprint(
	ctx context.Context,
	tx database.Tx,
	c courses.Course,
	a settings.AI,
	ready *revision,
) (blueprints.Blueprint, map[string]map[string]any, map[string]any, string, error) {
	stored, err := tx.Navigation().BlueprintPayload(ctx, ready.ID)
	if err != nil {
		return blueprints.Blueprint{}, nil, nil, "", err
	}
	if stored.Payload == nil || stored.Packet == nil || len(*stored.Packet) > 8*1024*1024 {
		return blueprints.Blueprint{}, nil, nil, "cached_blueprint_payload_missing", nil
	}
	var packet map[string]any
	if err := decodeJSON([]byte(*stored.Packet), &packet); err != nil || packet == nil {
		return blueprints.Blueprint{}, nil, nil, "cached_evidence_packet_invalid", nil
	}
	validated, err := blueprints.Validate([]byte(*stored.Payload), packet)
	if err != nil {
		return blueprints.Blueprint{}, nil, nil, "cached_blueprint_invalid", nil
	}
	links, reason, err := freshEvidence(ctx, tx, c.ID, a, packet)
	if err != nil {
		return blueprints.Blueprint{}, nil, nil, "", err
	}
	if reason != "" {
		return blueprints.Blueprint{}, nil, nil, reason, nil
	}
	return validated, links, packet, "", nil
}

func emptyView(c courses.Course, a settings.AI, history []revision) map[string]any {
	state, reason := "waiting_for_sources", "Document insights are needed before a course blueprint can be prepared."
	enabled := a.CourseEnabled && a.EnrichmentEnabled
	status := "missing"
	generation := map[string]any{"enabled": enabled, "status": status, "pending": false, "usable_revision_id": nil}
	if len(history) > 0 {
		r := history[0]
		status = r.Status
		generation = map[string]any{
			"enabled":            enabled,
			"status":             status,
			"revision_id":        r.Hash,
			"revision_number":    r.Number,
			"attempts":           r.Attempts,
			"created_at":         r.Created,
			"generated_at":       r.Generated,
			"pending":            enabled && (status == "pending" || status == "running"),
			"usable_revision_id": nil,
		}
	}
	if status == "pending" || status == "running" {
		state, reason = "pending", "The first blueprint is being generated."
	}
	if status == "failed" {
		state, reason = "failed", "Blueprint generation failed."
	}
	if !enabled {
		state, reason = "disabled", "Course synthesis is disabled in settings."
	}
	return map[string]any{
		"course_id":   c.ID,
		"course_name": c.Name,
		"short_name":  c.ShortName,
		"usable":      false,
		"status":      status,
		"readiness": map[string]any{
			"state":  state,
			"reason": reason,
			"usable": false,
		},
		"revision_id":       nil,
		"revision_number":   nil,
		"generation":        generation,
		"blueprint":         nil,
		"warnings":          []any{},
		"validation_reason": nil,
	}
}

func invalidate(view map[string]any, reason string) map[string]any {
	view["validation_reason"] = reason
	view["readiness"] = map[string]any{
		"state":  "invalid",
		"reason": "The stored blueprint no longer matches current evidence.",
		"usable": false,
	}
	return view
}
