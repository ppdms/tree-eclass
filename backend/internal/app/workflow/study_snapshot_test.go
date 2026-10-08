package workflow

import (
	"testing"
	"time"

	"tree-eclass/internal/domain/study"
)

func studySnapshotChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Native.Exec(
		ctx,
		`INSERT INTO app.courses(id,name,webdav_folder,hidden) VALUES(801,'Study visible','/Courses/801',0),(802,'Study hidden planned','/Courses/802',1),(803,'Study hidden inactive','/Courses/803',1);
 INSERT INTO app.course_exam_plans(course_id,enabled,exam_at) VALUES(802,1,'2026-09-20');
 INSERT INTO app.nodes(id,course_id,name,url,local_path) VALUES(9801,801,'root','https://example.invalid/801','/Courses/801/eclass'),(9802,802,'root','https://example.invalid/802','/Courses/802/eclass');
 INSERT INTO app.files(node_id,url,name,local_path,last_updated) VALUES
 (9801,'https://example.invalid/old','old.txt','/Courses/801/eclass/old.txt','2026-01-01'),
 (9801,'https://example.invalid/mastered','mastered.txt','/Courses/801/eclass/mastered.txt','2026-01-01'),
 (9801,'https://example.invalid/ignored','ignored.txt','/Courses/801/eclass/ignored.txt','2026-01-01'),
 (9802,'https://example.invalid/unknown','unknown.txt','/Courses/802/eclass/unknown.txt','invalid-upstream-date');
 INSERT INTO app.file_study(course_id,file_path,level) VALUES(801,'/Courses/801/eclass/mastered.txt',4),(801,'/Courses/801/eclass/ignored.txt',5)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	service := study.Service{Pool: pool}
	selected := int64(801)
	now := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	snapshot, err := service.Snapshot(ctx, &selected, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Courses) != 1 || snapshot.Selected == nil || snapshot.Selected.ID != 801 ||
		len(snapshot.Plans) != 1 ||
		len(snapshot.Inbox) != 1 ||
		snapshot.Inbox[0].Priority != 45 {
		t.Fatal("selected study scope or ignored-file priority", snapshot)
	}
	selected = 802
	snapshot, err = service.Snapshot(ctx, &selected, now)
	if err != nil || snapshot.Selected == nil || !snapshot.Selected.Hidden || len(snapshot.Inbox) != 1 ||
		snapshot.Inbox[0].Priority != 30 {
		t.Fatal("hidden commitment or invalid date handling", snapshot, err)
	}
	apiJSON(t, "GET", base+"/api/v1/study/snapshot?course_id=803", nil, 404, nil)
	apiJSON(t, "GET", base+"/api/v1/study/snapshot?course_id=not-an-id", nil, 422, nil)
	apiJSON(t, "GET", base+"/api/v1/study/snapshot?course_id=801", nil, 200, &snapshot)
	if snapshot.Selected == nil || snapshot.Selected.ID != 801 || snapshot.Settings.BlockMinutes != 45 {
		t.Fatal("snapshot lost saved planner settings", snapshot)
	}
	apiJSON(t, "GET", base+"/api/v1/study/snapshot", nil, 200, &snapshot)
	if snapshot.Selected != nil {
		t.Fatal("unselected study fell back to prior course")
	}
	for _, item := range snapshot.Courses {
		if item.Hidden {
			t.Fatal("hidden course appeared in default shelf")
		}
	}
	for _, item := range snapshot.Inbox {
		if item.CourseID == 802 {
			t.Fatal("hidden study files leaked into unscoped inbox")
		}
	}
	_, err = pool.Native.Exec(ctx, `INSERT INTO app.files(node_id,url,name,local_path,last_updated)
 SELECT 9801,'https://example.invalid/future-'||i,'future-'||i||'.txt','/Courses/801/eclass/future-'||i||'.txt','2100-01-01' FROM generate_series(1,65) i`)
	if err != nil {
		t.Fatal(err)
	}
	selected = 801
	snapshot, err = service.Snapshot(ctx, &selected, now)
	if err != nil || len(snapshot.Inbox) != 60 || snapshot.Inbox[0].Name != "old.txt" ||
		snapshot.Inbox[1].Priority != 0 {
		t.Fatal("inbox bound, ordering or future timestamps", err)
	}
	if _, err = pool.Native.Exec(ctx, `DELETE FROM app.courses WHERE id IN(801,802,803)`); err != nil {
		t.Fatal(err)
	}
}
