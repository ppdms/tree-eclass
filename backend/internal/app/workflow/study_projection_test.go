package workflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/study"
)

func studyProjectionChecks(t *testing.T, pool *pgxpool.Pool, base, action string) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	service := study.Service{Pool: pool}
	exam := now.AddDate(0, 0, 10).Format(time.DateOnly)
	if _, err := pool.Exec(ctx, `INSERT INTO app.course_exam_plans(course_id,enabled,exam_at) VALUES(101,1,$1)`, exam); err != nil {
		t.Fatal(err)
	}
	// The exam-plan mutation invalidates the navigation as well as the schedule.
	refreshNavigation(t, pool)
	refreshStudy := func() {
		t.Helper()
		for range 100 {
			changed, err := service.Refresh(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			if !changed {
				return
			}
		}
		t.Fatal("study projection did not settle")
	}
	var view map[string]any
	apiJSON(t, "GET", base+"/api/v1/study/intelligence", nil, 200, &view)
	if view["study_projection_status"] != "pending" {
		t.Fatal("unbuilt projection claimed current", view)
	}
	refreshStudy()
	for _, path := range []string{"/api/v1/study", "/api/v1/study/", "/api/v1/study/intelligence", "/api/v1/study?course_id=101"} {
		apiJSON(t, "GET", base+path, nil, 200, &view)
		if view["study_projection_status"] != "ready" || view["adaptive_plan_available"] != true {
			t.Fatal("built study projection unavailable", path, view)
		}
		adaptive := view["adaptive_plan"].(map[string]any)
		if adaptive["total_required_minutes"].(float64) <= 0 || len(adaptive["actions"].([]any)) != 1 ||
			adaptive["next_session"] == nil {
			t.Fatal("saved native blueprint was not scheduled", adaptive)
		}
		coverage := view["study_intelligence"].(map[string]any)["coverage"].(map[string]any)
		if coverage["total"] != float64(1) || coverage["enriched"] != float64(1) {
			t.Fatal("current indexed material missing from intelligence", coverage)
		}
	}
	apiJSON(t, "OPTIONS", base+"/api/v1/study/", nil, 200, nil)
	apiJSON(t, "GET", base+"/api/v1/study/intelligence?course_id=999999", nil, 404, nil)
	apiJSON(t, "GET", base+"/api/v1/study?course_id=bad", nil, 422, nil)
	studyProjectionProgress(t, pool, base, action, refreshStudy)
	nextDay, err := service.Intelligence(ctx, nil, now.AddDate(0, 0, 1))
	if err != nil || nextDay["study_projection_status"] != "pending" {
		t.Fatal("calendar rollover reused yesterday's schedule", nextDay, err)
	}
	for _, query := range []string{`DELETE FROM app.study_unit_events WHERE idempotency_key='study-projection-complete'`, `DELETE FROM app.course_exam_plans WHERE course_id=101`, `DELETE FROM read_model.study_metrics`} {
		if _, err := pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	refreshNavigation(t, pool)
}

func studyProjectionProgress(t *testing.T, pool *pgxpool.Pool, base, action string, refresh func()) {
	t.Helper()
	var response map[string]any
	event := study.Event{
		CourseID: 101,
		Action:   action,
		Revision: "build-r1",
		Type:     "completed",
		Key:      "study-projection-complete",
	}
	apiJSON(t, "POST", base+"/api/study/actions/event", event, 200, &response)
	if response["status"] != "recorded" || response["plan"] != nil || response["plan_error"] == nil {
		t.Fatal("legacy mutation returned stale plan or hid committed event", response)
	}
	var view map[string]any
	apiJSON(t, "GET", base+"/api/v1/study/intelligence", nil, 200, &view)
	if view["study_projection_status"] != "pending" {
		t.Fatal("learner event failed to invalidate schedule", view)
	}
	refresh()
	apiJSON(t, "GET", base+"/api/v1/study/intelligence", nil, 200, &view)
	plan := view["adaptive_plan"].(map[string]any)
	if plan["total_required_minutes"] != float64(0) || plan["next_session"] != nil {
		raw, _ := json.Marshal(plan)
		t.Fatal("completed action remained scheduled", string(raw))
	}
	apiJSON(t, "POST", base+"/api/study/actions/event", event, 200, &response)
	if response["plan"] == nil || response["plan_error"] != nil {
		t.Fatal("idempotent retry unnecessarily invalidated ready plan", response)
	}
}
