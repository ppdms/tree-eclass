package study

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"

	"tree-eclass/internal/domain/settings"
)

func TestPrioritySelection(t *testing.T) {
	raw, err := os.ReadFile("testdata/priorities.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Selected  *int64
		Materials []PriorityMaterial
		Plans     []settings.ExamPlan
		Levels    map[string]map[string]int64
		Expected  map[string]any
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	today, _ := examDate("2026-09-12")
	for _, f := range fixtures {
		p := newPriorities(f.Plans, f.Selected, today)
		for i := range f.Materials {
			m := &f.Materials[i]
			m.Level = f.Levels[strconv.FormatInt(m.CourseID, 10)][m.Path]
			p.Add(m)
		}
		raw, _ := json.Marshal(p.result())
		var actual map[string]any
		if err = json.Unmarshal(raw, &actual); err != nil {
			t.Fatal(err)
		}
		// The native material type includes explicit nullable fields and the same
		// reading recommendation in both runway and queue entries. Compare the
		// independent ranking/count/coverage contract, not incidental row shapes.
		if !reflect.DeepEqual(actual["coverage"], f.Expected["coverage"]) {
			t.Fatal("priority coverage")
		}
		compareMaterialOrder(t, actual["focus_queue"], f.Expected["focus_queue"])
		a, e := actual["exam_runways"].([]any), f.Expected["exam_runways"].([]any)
		if len(a) != len(e) {
			t.Fatal("runway scope")
		}
		for i := range a {
			x, y := a[i].(map[string]any), e[i].(map[string]any)
			for _, key := range []string{"course_id", "readiness", "remaining_count", "essential_remaining", "days_left"} {
				if x[key] != y[key] {
					t.Fatal("runway metric", key, x[key], y[key])
				}
			}
			compareMaterialOrder(t, x["next_materials"], y["next_materials"])
		}
		collisions, expected := actual["exam_collisions"].([]any), f.Expected["exam_collisions"].([]any)
		if len(collisions) != len(expected) {
			t.Fatal("collision scope")
		}
		for i := range collisions {
			x, y := collisions[i].(map[string]any), expected[i].(map[string]any)
			if x["gap_days"] != y["gap_days"] || x["message"] != y["message"] {
				t.Fatal("collision", x, y)
			}
		}
	}
}

func compareMaterialOrder(t *testing.T, actual, expected any) {
	t.Helper()
	a, e := actual.([]any), expected.([]any)
	if len(a) != len(e) {
		t.Fatal("candidate count", len(a), len(e))
	}
	for i := range a {
		x, y := a[i].(map[string]any), e[i].(map[string]any)
		if x["id"] != y["id"] || x["priority"] != y["priority"] || x["level"] != y["level"] {
			t.Fatal("candidate order", x, y)
		}
	}
}

func TestIgnoredMaterialDoesNotInflateReadiness(t *testing.T) {
	today, _ := examDate("2026-09-12")
	exam := "2026-09-15"
	p := newPriorities(
		[]settings.ExamPlan{{CourseID: 101, CourseName: "Course", Enabled: true, ExamAt: &exam}},
		nil,
		today,
	)
	p.Add(&PriorityMaterial{ID: "ignored", CourseID: 101, Name: "Ignored", Level: 5, AI: map[string]any{}})
	p.Add(&PriorityMaterial{ID: "unseen", CourseID: 101, Name: "Unseen", Level: 0, AI: map[string]any{}})
	view := p.result()
	runway := view["exam_runways"].([]map[string]any)[0]
	if runway["readiness"] != int64(0) || runway["remaining_count"] != int64(1) {
		t.Fatal("ignored material inflated readiness", runway)
	}
	queue := view["focus_queue"].([]*PriorityMaterial)
	if len(queue) != 1 || queue[0].ID != "unseen" {
		t.Fatal("ignored material scheduled", queue)
	}
}
