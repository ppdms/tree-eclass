package workflow

import (
	"fmt"
	"testing"

	"tree-eclass/internal/domain/activity"
	"tree-eclass/internal/domain/identity"
)

func activityChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	_, err := pool.Native.Exec(t.Context(), `INSERT INTO app.announcements(course_id,announcement_id,title,link,pub_date)
SELECT 101,'synthetic-'||i,'Announcement '||i,'https://example.invalid/'||i,'2026-09-12T09:00:00Z' FROM generate_series(1,12) i;
INSERT INTO app.announcements(course_id,announcement_id,title,link,pub_date) VALUES(102,'hidden','Must not appear','https://example.invalid','2027-01-01');
INSERT INTO app.global_announcements(feed_key,announcement_id,title,link,pub_date) VALUES('dept','global','Exam deadline','https://example.invalid','2026-09-12T10:00:00Z');
INSERT INTO app.change_records(id,course_id,change_no,timestamp,message) VALUES(990,101,'synthetic-change','2026-09-12T11:00:00Z','+1');
INSERT INTO app.change_record_items(change_record_id,change_type,file_path,display_name) VALUES(990,'newfile','/Courses/101/eclass/notes.txt','notes.txt')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(t.Context(), `UPDATE app.announcements SET title=$1 WHERE announcement_id='synthetic-12'`, identity.Encode("Σημείωση\x00\ue000")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var first activity.Page
	apiJSON(t, "GET", base+"/api/v1/inbox?limit=10", nil, 200, &first)
	if first.Timeline == nil || len(*first.Timeline) != 10 || !first.More || first.Next != 10 {
		t.Fatal("activity page boundary", first.Next, first.More)
	}
	if (*first.Timeline)[0].Type != "change" || len((*first.Timeline)[0].Changes) != 1 ||
		(*first.Timeline)[1].CourseName != "Dept" ||
		(*first.Timeline)[2].Title != "Σημείωση\x00\ue000" {
		t.Fatal("event ordering, source bodies or text changed")
	}
	for _, item := range *first.Timeline {
		seen[item.Type+string(item.ID)] = true
	}
	var second activity.Page
	apiJSON(t, "GET", fmt.Sprintf("%s/api/v1/inbox?limit=10&offset=%d", base, first.Next), nil, 200, &second)
	if second.Timeline == nil || len(*second.Timeline) != 4 || second.More || second.Next != 14 {
		t.Fatal("final page", second.Next, second.More)
	}
	for _, item := range *second.Timeline {
		key := item.Type + string(item.ID)
		if seen[key] {
			t.Fatal("unstable tie ordering repeated an event")
		}
		seen[key] = true
	}
	var lean activity.Page
	apiJSON(t, "GET", base+"/api/v1/inbox?include_timeline=false", nil, 200, &lean)
	if lean.Timeline != nil || len(lean.Groups) != 14 {
		t.Fatal("lean activity contract changed")
	}
	apiJSON(t, "GET", base+"/api/v1/timeline/", nil, 200, nil)
	apiJSON(t, "GET", base+"/api/v1/inbox?offset=9223372036854775807", nil, 422, nil)
	var updates activity.CoursePage
	apiJSON(t, "GET", base+"/api/v1/courses/101/updates?limit=1", nil, 200, &updates)
	if len(updates.Timeline) != 1 || updates.Timeline[0].Type != "change" || len(updates.Timeline[0].Changes) != 0 ||
		!updates.More {
		t.Fatal("course updates loaded change items or lost pagination", updates)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/101/updates?offset=1", nil, 200, &updates)
	if updates.Offset != 1 || len(updates.Timeline) != 10 || updates.Timeline[0].Title != "Σημείωση\x00\ue000" {
		t.Fatal("course update order or text", updates)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/101/updates?offset=11", nil, 200, &updates)
	if updates.More || len(updates.Timeline) != 2 {
		t.Fatal("course final page", updates)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/102/updates", nil, 404, nil)
	apiJSON(t, "GET", base+"/api/v1/courses/101/updates?limit=51", nil, 422, nil)
}
