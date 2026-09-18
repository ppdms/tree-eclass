package workflow

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func navigationChecks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(711,'Synthetic roadmap','/Courses/711')`); err != nil {
		t.Fatal(err)
	}
	service := navigation.Service{Pool: pool}
	request := navigation.Request{CourseID: 711}
	view, err := service.Read(ctx, request)
	if err != nil || view.Blueprint["usable"] != false {
		t.Fatal("unpublished navigation", view, err)
	}
	a, err := (settings.Service{Pool: pool}).AI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var generation int64
	if err = pool.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=711`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"course_id":   711,
		"usable":      true,
		"revision_id": "synthetic-r1",
		"readiness":   map[string]any{"state": "ready"},
		"blueprint": map[string]any{
			"exam_strategy": map[string]any{
				"summary": "Read evidence",
			}, "question_families": []any{}, "units": []any{map[string]any{"key": "u1", "title": "Μονάδα", "objective": "Objective", "actions": []any{
				map[string]any{
					"action_id":         "a1",
					"estimated_minutes": 10,
					"title":             "Read",
					"status":            "completed",
					"progress_minutes":  10,
				},
				map[string]any{"action_id": "a2", "estimated_minutes": 20, "title": "Recall"},
			}}}},
	}
	if published, err := service.Publish(ctx, 711, generation-1, a, payload); err != nil || published {
		t.Fatal("obsolete snapshot publication", err)
	}
	if published, err := service.Publish(ctx, 711, generation, a, payload); err != nil || !published {
		t.Fatal("navigation publication", err)
	}
	navigationProgressChecks(t, pool, service, a, payload, generation)
}

func navigationProgressChecks(
	t *testing.T,
	pool *pgxpool.Pool,
	service navigation.Service,
	a settings.AI,
	payload map[string]any,
	generation int64,
) {
	t.Helper()
	ctx := t.Context()
	request := navigation.Request{CourseID: 711}
	var err error
	var contentID string
	var view navigation.View
	if err = pool.QueryRow(ctx, `SELECT content_id FROM read_model.navigation WHERE course_id=711`).Scan(&contentID); err != nil {
		t.Fatal(err)
	}
	view, err = service.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	progress := view.Blueprint["progress"].(map[string]any)
	if progress["total_actions"] != int64(2) || progress["completed_actions"] != int64(0) {
		t.Fatal("publication trusted embedded learner state", progress)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO app.study_unit_events(course_id,plan_revision,action_id,unit_key,event_type,actual_minutes) VALUES(711,'synthetic-r1','a1','u1','partial',4),(711,'synthetic-r1','a1','u1','partial',3)`); err != nil {
		t.Fatal(err)
	}
	view, err = service.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	next := view.Blueprint["progress"].(map[string]any)["next_action"].(map[string]any)
	if next["progress_minutes"] != int64(7) || next["remaining_minutes"] != int64(3) {
		t.Fatal("live partial progress", next)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO app.study_unit_events(course_id,plan_revision,action_id,unit_key,event_type) VALUES(711,'synthetic-r1','a1','u1','completed')`); err != nil {
		t.Fatal(err)
	}
	view, err = service.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	progress = view.Blueprint["progress"].(map[string]any)
	if progress["percent"] != int64(50) || progress["next_action"].(map[string]any)["action_id"] != "a2" {
		t.Fatal("live completed progress", progress)
	}
	var afterID string
	var afterGeneration int64
	if err = pool.QueryRow(ctx, `SELECT n.content_id,g.generation FROM read_model.navigation n JOIN read_model.course_generation g USING(course_id) WHERE course_id=711`).Scan(&afterID, &afterGeneration); err != nil ||
		afterID != contentID ||
		afterGeneration != generation {
		t.Fatal("learner event rewrote immutable plan", err)
	}
	navigationDeferredChecks(t, pool, service, a, payload, generation, contentID, request)
}

func navigationDeferredChecks(
	t *testing.T,
	pool *pgxpool.Pool,
	service navigation.Service,
	a settings.AI,
	payload map[string]any,
	generation int64,
	contentID string,
	request navigation.Request,
) {
	t.Helper()
	ctx := t.Context()
	var view navigation.View
	var err error
	request.Roadmap = true
	view, err = service.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if view.Blueprint["actions"] != nil || view.Blueprint["actions_deferred"] != true ||
		view.Blueprint["blueprint"].(map[string]any)["exam_strategy"] != nil {
		t.Fatal("summary loaded deferred action/strategy fields")
	}
	request.IncludeActions = true
	unit := "u1"
	request.Unit = &unit
	view, err = service.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	actions := view.Blueprint["actions"].([]map[string]any)
	if len(actions) != 2 || actions[0]["progress_minutes"] != int64(10) ||
		view.Blueprint["progress"].(map[string]any)["next_action_id"] != "a2" {
		t.Fatal("roadmap action progress", actions)
	}
	navigationFreshnessChecks(t, pool, service, a, payload, generation, contentID)
}

func navigationFreshnessChecks(
	t *testing.T,
	pool *pgxpool.Pool,
	service navigation.Service,
	a settings.AI,
	payload map[string]any,
	generation int64,
	contentID string,
) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `UPDATE app.courses SET name='Changed source' WHERE id=711`); err != nil {
		t.Fatal(err)
	}
	view, err := service.Read(ctx, navigation.Request{CourseID: 711})
	if err != nil || view.Blueprint["usable"] != false {
		t.Fatal("committed source change did not hide stale navigation", err)
	}
	if published, err := service.Publish(ctx, 711, generation, a, payload); err != nil || published {
		t.Fatal("late processor replaced newer input", err)
	}
	if err = pool.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=711`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	changed := a
	changed.CourseModel = "changed-model"
	if err = (settings.Service{Pool: pool}).SaveAI(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=711`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if published, err := service.Publish(ctx, 711, generation, a, payload); err != nil || published {
		t.Fatal("old AI configuration published as current", err)
	}
	payload["revision_id"] = "synthetic-r2"
	if published, err := service.Publish(ctx, 711, generation, changed, payload); err != nil || !published {
		t.Fatal("new configuration publication", err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM read_model.roadmap_content WHERE content_id=$1`, contentID).Scan(&count); err != nil ||
		count != 0 {
		t.Fatal("superseded content retained", err)
	}
	var currentID string
	if err = pool.QueryRow(ctx, `SELECT content_id FROM read_model.navigation WHERE course_id=711`).Scan(&currentID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `DELETE FROM app.courses WHERE id=711`); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM read_model.roadmap_content WHERE content_id=$1`, currentID).Scan(&count); err != nil ||
		count != 0 {
		t.Fatal("deleted course retained immutable content", err)
	}
	if err = (settings.Service{Pool: pool}).SaveAI(ctx, a); err != nil {
		t.Fatal(err)
	}
}
