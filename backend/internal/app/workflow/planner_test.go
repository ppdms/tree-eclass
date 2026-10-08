package workflow

import (
	"errors"
	"net/url"
	"testing"

	"tree-eclass/internal/domain/settings"
)

func plannerChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Native.Exec(
		ctx,
		`INSERT INTO app.courses(id,name,webdav_folder,hidden) VALUES(102,'Κρυφό','/Courses/102',1),(103,'Ανενεργό','/Courses/103',1);
INSERT INTO app.course_exam_plans(course_id,enabled,remaining_blocks,importance,max_daily_blocks) VALUES(101,0,12,2.5,4),(102,1,3,1,2);
INSERT INTO app.nodes(course_id,name,url,local_path) VALUES(101,'folder','https://example.invalid/folder','/Courses/101/eclass/folder')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	service := settings.Service{Pool: pool}
	form := url.Values{
		"weekly_minutes_0":   {"90"},
		"block_minutes":      {"45"},
		"blackout_dates":     {"2026-10-01,2026-09-20 2026-10-01"},
		"enabled_101":        {"on"},
		"exam_at_101":        {"2026-09-25T10:00"},
		"commitment_101":     {"must_pass"},
		"target_grade_101":   {"8.5"},
		"short_name_101":     {"Δένδρα\x00"},
		"planning_notes_101": {"Σημειώσεις"},
		"enabled_102":        {"on"},
		"exam_at_102":        {"2026-09-26T11:00"},
	}
	payload := map[string]string{}
	for key, values := range form {
		payload[key] = values[0]
	}
	apiJSON(t, "POST", base+"/api/v1/study/planner", payload, 200, nil)
	planner, err := service.Planner(ctx)
	if err != nil || planner.Weekly["0"] != 90 || planner.Weekly["1"] != 150 || planner.BlockMinutes != 45 ||
		len(planner.Blackouts) != 2 ||
		planner.Blackouts[0] != "2026-09-20" {
		t.Fatal("planner settings", planner, err)
	}
	plans, err := service.ExamPlans(ctx)
	if err != nil || len(plans) != 2 || !plans[1].Enabled || plans[0].Remaining != 12 || plans[0].Importance != 2.5 ||
		plans[0].MaxBlocks != 4 ||
		plans[0].TargetGrade != 8.5 ||
		plans[0].ShortName == nil ||
		*plans[0].ShortName != "Δένδρα\x00" {
		t.Fatal("exam commitments or advanced values lost", plans, err)
	}
	form.Set("weekly_minutes_0", "120")
	form.Set("target_grade_102", "NaN")
	var issues settings.PlannerErrors
	if err = service.SavePlanner(ctx, form); !errors.As(err, &issues) {
		t.Fatal("non-finite grade accepted", err)
	}
	planner, err = service.Planner(ctx)
	if err != nil || planner.Weekly["0"] != 90 {
		t.Fatal("invalid exam partially saved planner", err)
	}
	form.Set("target_grade_102", "7")
	form.Set("blackout_dates", "2026-02-30")
	if err = service.SavePlanner(ctx, form); !errors.As(err, &issues) {
		t.Fatal("invalid blackout accepted", err)
	}
	form.Set("blackout_dates", "")
	form.Set("planning_notes_101", "")
	form.Del("enabled_102")
	if err = service.SavePlanner(ctx, form); err != nil {
		t.Fatal(err)
	}
	plans, err = service.ExamPlans(ctx)
	if err != nil || len(plans) != 1 || plans[0].Notes != nil {
		t.Fatal("explicit clear or hidden commitment disable failed", plans, err)
	}
	plannerRouteChecks(t, base)
}

func plannerRouteChecks(t *testing.T, base string) {
	t.Helper()
	apiJSON(
		t,
		"POST",
		base+"/api/courses/101/files/study-level",
		map[string]any{"file_path": "/Courses/101/eclass/old.txt", "level": 5},
		200,
		nil,
	)
	apiJSON(
		t,
		"POST",
		base+"/api/courses/101/files/study-level",
		map[string]any{"file_path": "path", "level": true},
		422,
		nil,
	)
	apiJSON(
		t,
		"POST",
		base+"/api/courses/102/files/study-level",
		map[string]any{"file_path": "path", "level": 2},
		404,
		nil,
	)
	apiJSON(
		t,
		"POST",
		base+"/api/courses/101/folders/collapsed",
		map[string]any{"folder_key": "/Courses/101/eclass/folder", "collapsed": true},
		200,
		nil,
	)
	apiJSON(
		t,
		"POST",
		base+"/api/courses/101/folders/collapsed",
		map[string]any{"folder_key": "/Courses/101/eclass/missing", "collapsed": true},
		404,
		nil,
	)
	apiJSON(
		t,
		"POST",
		base+"/api/courses/101/folders/collapsed",
		map[string]any{"folder_key": "https://example.invalid/folder", "collapsed": false},
		200,
		nil,
	)
}
