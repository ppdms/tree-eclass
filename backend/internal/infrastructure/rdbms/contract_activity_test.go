package rdbms_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"tree-eclass/internal/domain/activity"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func TestActivityPaginationPreservesOrderingAndVisibility(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			ctx := t.Context()
			service := courses.Service{Pool: store}
			if err := service.Add(ctx, 101, "Snapshot feeds", "Feeds"); err != nil {
				t.Fatal(err)
			}
			if err := service.Add(ctx, 102, "Hidden feeds"); err != nil {
				t.Fatal(err)
			}
			if err := service.Hide(ctx, 102, true); err != nil {
				t.Fatal(err)
			}
			tx, err := store.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			seedActivityFeeds(t, ctx, tx)
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			reader := activity.Reader{Pool: store}
			assertGlobalActivityPages(t, reader)
			assertCourseActivityPages(t, reader)
		})
	}
}

func assertGlobalActivityPages(t *testing.T, reader activity.Reader) {
	t.Helper()
	ctx := t.Context()
	first, err := reader.Page(ctx, 10, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Timeline == nil || len(*first.Timeline) != 10 || !first.More || first.Next != 10 {
		t.Fatalf("activity boundary = %+v", first)
	}
	head := *first.Timeline
	if head[0].Type != "change" || len(head[0].Changes) != 1 ||
		head[1].CourseName != "Dept" || head[2].Title != "Σημείωση\x00\ue000" {
		t.Fatal("event ordering, source bodies or text changed")
	}
	if head[0].CourseID == nil || *head[0].CourseID != 101 {
		t.Fatalf("change course = %+v", head[0])
	}
	if head[1].CourseID != nil {
		t.Fatalf("global course id = %+v", head[1].CourseID)
	}
	seen := map[string]bool{}
	for _, item := range head {
		seen[item.Type+string(item.ID)] = true
	}
	second, err := reader.Page(ctx, 10, first.Next, true)
	if err != nil {
		t.Fatal(err)
	}
	if second.Timeline == nil || len(*second.Timeline) != 4 || second.More || second.Next != 14 {
		t.Fatalf("activity tail = %+v", second)
	}
	for _, item := range *second.Timeline {
		key := item.Type + string(item.ID)
		if seen[key] {
			t.Fatal("unstable tie ordering repeated an event")
		}
	}
	if _, err := reader.Page(ctx, 10, 1<<31, true); err == nil {
		t.Fatal("oversize activity offset admitted")
	}
}

func assertCourseActivityPages(t *testing.T, reader activity.Reader) {
	t.Helper()
	ctx := t.Context()
	updates, err := reader.CoursePage(ctx, 101, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates.Timeline) != 1 || updates.Timeline[0].Type != "change" ||
		len(updates.Timeline[0].Changes) != 0 || !updates.More {
		t.Fatalf("course updates = %+v", updates)
	}
	updates, err = reader.CoursePage(ctx, 101, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updates.Offset != 1 || len(updates.Timeline) != 10 ||
		updates.Timeline[0].Title != "Σημείωση\x00\ue000" {
		t.Fatalf("course update order = %+v", updates)
	}
	updates, err = reader.CoursePage(ctx, 101, 10, 11)
	if err != nil {
		t.Fatal(err)
	}
	if updates.More || len(updates.Timeline) != 2 {
		t.Fatalf("course tail = %+v", updates)
	}
	if _, err = reader.CoursePage(ctx, 102, 10, 0); !errors.Is(err, database.ErrNoRows) {
		t.Fatalf("hidden course updates error = %v", err)
	}
}

func seedActivityFeeds(t *testing.T, ctx context.Context, tx database.Tx) {
	t.Helper()
	for i := 1; i <= 12; i++ {
		title := fmt.Sprintf("Announcement %d", i)
		if i == 12 {
			title = identity.Encode("Σημείωση\x00\ue000")
		}
		published := "2026-09-12T09:00:00Z"
		if err := tx.Sync().UpsertAnnouncement(ctx, database.SyncAnnouncementInput{
			CourseID: 101, ID: fmt.Sprintf("synthetic-%d", i), Title: title,
			Link: fmt.Sprintf("https://example.invalid/%d", i), Published: &published,
		}); err != nil {
			t.Fatal(err)
		}
	}
	hiddenPublished := "2027-01-01"
	if err := tx.Sync().UpsertAnnouncement(ctx, database.SyncAnnouncementInput{
		CourseID: 102, ID: "hidden", Title: "Must not appear",
		Link: "https://example.invalid", Published: &hiddenPublished,
	}); err != nil {
		t.Fatal(err)
	}
	globalPublished := "2026-09-12T10:00:00Z"
	if err := tx.Sync().UpsertGlobalAnnouncement(ctx, database.SyncGlobalAnnouncementInput{
		FeedKey: "dept", ID: "global", Title: "Exam deadline",
		Link: "https://example.invalid", Published: &globalPublished,
	}); err != nil {
		t.Fatal(err)
	}
	record, err := tx.Sync().InsertChangeRecord(ctx, 101, "synthetic-change", "+1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Sync().InsertChangeRecordItem(ctx, database.SyncChangeRecordItemInput{
		RecordID: record, Type: "newfile", Path: "/Courses/101/eclass/notes.txt", Name: "notes.txt",
	}); err != nil {
		t.Fatal(err)
	}
}
