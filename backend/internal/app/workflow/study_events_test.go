package workflow

import (
	"sync"
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/study"
)

func studyEventChecks(t *testing.T, pool rdbms.Pool, base, action string) {
	t.Helper()
	ctx := t.Context()
	minutes := int64(7)
	note := "Δένδρα\x00\ue0000"
	in := study.Event{
		CourseID: 101,
		Action:   action,
		Revision: "build-r1",
		Type:     "partial",
		Key:      "native-progress-001",
		Minutes:  &minutes,
		Note:     &note,
	}
	service := study.Service{Pool: pool}
	var wg sync.WaitGroup
	results := make(chan study.Event, 12)
	errors := make(chan error, 12)
	for range 12 {
		wg.Go(func() { result, err := service.Record(ctx, in); results <- result; errors <- err })
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id int64
	for result := range results {
		if id == 0 {
			id = result.ID
		}
		if result.ID != id || result.Note == nil || *result.Note != note {
			t.Fatal("concurrent retry changed event identity/text", result)
		}
	}
	var count, total int64
	if err := pool.QueryRow(ctx, `SELECT count(*),sum(actual_minutes) FROM app.study_unit_events WHERE action_id=$1`, action).Scan(&count, &total); err != nil ||
		count != 1 ||
		total != 7 {
		t.Fatal("retry duplicated learner time", count, total, err)
	}
	url := base + "/api/v1/study/actions/event"
	var response map[string]any
	apiJSON(t, "POST", url, in, 200, &response)
	if response["event_id"] != float64(id) {
		t.Fatal("HTTP retry changed event", response)
	}
	in.Type = "completed"
	apiJSON(t, "POST", url, in, 409, nil)
	in.Key = "native-progress-002"
	apiJSON(t, "POST", url, in, 200, &response)
	view, err := (navigation.Service{Pool: pool}).Read(ctx, navigation.Request{CourseID: 101})
	if err != nil || view.Blueprint["progress"].(map[string]any)["percent"] != int64(100) {
		t.Fatal("event did not immediately update overview", view, err)
	}
	in.Key = "native-progress-003"
	in.Revision = "obsolete"
	apiJSON(t, "POST", url, in, 409, nil)
	in.Revision = "build-r1"
	in.Action = "other-course-action"
	apiJSON(t, "POST", url, in, 409, nil)
	in.Action = action
	in.Type = "partial"
	in.Minutes = nil
	apiJSON(t, "POST", url, in, 422, nil)
	if _, err = pool.Exec(ctx, `DELETE FROM app.study_unit_events WHERE action_id=$1`, action); err != nil {
		t.Fatal(err)
	}
}
