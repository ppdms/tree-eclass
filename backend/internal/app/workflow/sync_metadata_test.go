package workflow

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/services/synchronization"
)

func syncMetadataChecks(t *testing.T, pool *fixtureStore, service synchronization.Service) {
	t.Helper()
	ctx := t.Context()
	ex := eclass.Exercise{ID: "42", Title: "Εργασία", SubmissionStatus: "pending", Grade: "7"}
	if err := service.SaveExercises(ctx, 101, []eclass.Exercise{ex}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.Exec(ctx, `UPDATE app.exercises SET ignored=1 WHERE exercise_id='42'`); err != nil {
		t.Fatal(err)
	}
	ex.Grade = "9"
	ex.Description = "Δένδρα\x00 και γράφοι"
	if err := service.SaveExercises(ctx, 101, []eclass.Exercise{ex}); err != nil {
		t.Fatal(err)
	}
	var grade string
	var ignored int
	if err := pool.Native.QueryRow(ctx, `SELECT grade,ignored FROM app.exercises WHERE exercise_id='42'`).Scan(&grade, &ignored); err != nil ||
		grade != "9" ||
		ignored != 1 {
		t.Fatal("exercise refresh lost grade or ignore", grade, ignored, err)
	}
	a := eclass.Announcement{ID: "stable-guid", Title: "Original", Link: "https://example.invalid/announcement"}
	if err := service.SaveAnnouncements(ctx, 101, []eclass.Announcement{a}); err != nil {
		t.Fatal(err)
	}
	a.Title = "Changed"
	if err := service.SaveAnnouncements(ctx, 101, []eclass.Announcement{a, {Title: "Missing identity"}}); err == nil {
		t.Fatal("invalid observation committed")
	}
	var title string
	if err := pool.Native.QueryRow(ctx, `SELECT title FROM app.announcements WHERE announcement_id='stable-guid'`).Scan(&title); err != nil ||
		title != "Original" {
		t.Fatal("failed feed partially replaced rows", title, err)
	}
	versions, err := service.Versions(ctx, 101, "modified", new("notes.txt"), nil)
	if err != nil || len(versions) != 2 {
		t.Fatal("modified history", versions, err)
	}
	deleted, err := service.Versions(ctx, 101, "deleted", nil, new("other"))
	if err != nil || len(deleted) != 0 {
		t.Fatal("folder boundary", deleted, err)
	}
	var number string
	if err = pool.Native.QueryRow(ctx, `SELECT change_no FROM app.change_records ORDER BY id DESC LIMIT 1`).Scan(&number); err != nil {
		t.Fatal(err)
	}
	record, items, err := service.History(ctx, 101, number)
	if err != nil || record.Count != 1 || len(items) != 1 || items[0].Type != "deleted_file" {
		t.Fatal("history detail", record, items, err)
	}
	syncAdmissionChecks(t, pool, service)
}

func syncAdmissionChecks(t *testing.T, pool *fixtureStore, service synchronization.Service) {
	t.Helper()
	ctx := t.Context()
	var admitted atomic.Int32
	var wait sync.WaitGroup
	for range 6 {
		wait.Go(func() {
			_, err := service.Enqueue(ctx, new(int64(101)))
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, synchronization.ErrBusy) {
				t.Error(err)
			}
		})
	}
	wait.Wait()
	if admitted.Load() != 1 {
		t.Fatal("concurrent checks admitted", admitted.Load())
	}
	status, err := (settings.Service{Pool: pool}).Check(ctx)
	if err != nil || !status.IsChecking || status.CourseID == nil || *status.CourseID != 101 {
		t.Fatal(status, err)
	}
	if err = service.Finish(ctx, synchronization.Result{FilesAdded: 2, FilesChanged: 1}, nil); err != nil {
		t.Fatal(err)
	}
	status, err = (settings.Service{Pool: pool}).Check(ctx)
	if err != nil || status.IsChecking || status.FilesAdded == nil || *status.FilesAdded != 2 {
		t.Fatal(status, err)
	}
	syncRetryChecks(t, pool, service)
}
