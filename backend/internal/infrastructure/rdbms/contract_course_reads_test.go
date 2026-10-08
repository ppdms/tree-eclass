package rdbms_test

import (
	"errors"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func TestCourseReadsUseVisibleSnapshot(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			ctx := t.Context()
			service := courses.Service{Pool: store}
			if err := service.Add(ctx, 101, "Snapshot course", "Snap"); err != nil {
				t.Fatal(err)
			}
			if err := service.Add(ctx, 102, "Hidden course"); err != nil {
				t.Fatal(err)
			}
			if err := service.Hide(ctx, 102, true); err != nil {
				t.Fatal(err)
			}
			seedCourseCatalog(t, store)
			tree, err := service.Tree(ctx, 101)
			if err != nil {
				t.Fatal(err)
			}
			if tree.Name != "root" || len(tree.Children) != 1 ||
				tree.Children[0].Name != "notes" || len(tree.Children[0].Files) != 1 ||
				tree.Children[0].Files[0].Name != "file.txt" {
				t.Fatalf("course tree = %+v", tree)
			}
			files, err := service.Files(ctx, 101)
			if err != nil {
				t.Fatal(err)
			}
			if files.Tree == nil || files.Study["/Courses/101/eclass/notes/file.txt"] != 3 {
				t.Fatalf("course files = %+v", files)
			}
			if _, err = service.Tree(ctx, 102); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("hidden tree error = %v", err)
			}
			if _, err = service.Files(ctx, 999); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("missing files error = %v", err)
			}
			guard, err := store.Courses().VisibleCourse(ctx, 101)
			if err != nil || guard.ID != 101 || guard.Hidden != 0 {
				t.Fatalf("visible course = %+v, %v", guard, err)
			}
			if _, err = store.Courses().VisibleCourse(ctx, 102); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("hidden guard error = %v", err)
			}
		})
	}
}

func seedCourseCatalog(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	root, err := tx.Sync().InsertDirectory(ctx, database.SyncDirectoryInput{
		CourseID: 101, Name: identity.Encode("root"),
		URL: "https://example.invalid/101", Path: identity.Encode("/Courses/101/eclass"),
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := tx.Sync().InsertDirectory(ctx, database.SyncDirectoryInput{
		CourseID: 101, ParentID: &root, Name: identity.Encode("notes"),
		URL: "https://example.invalid/101/notes", Path: identity.Encode("/Courses/101/eclass/notes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Sync().InsertFile(ctx, database.SyncFileInput{NodeID: child,
		URL: "https://example.invalid/101/file", Name: identity.Encode("file.txt"),
		Path: identity.Encode("/Courses/101/eclass/notes/file.txt")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := (courses.Service{Pool: store}).StudyLevel(ctx, 101, "/Courses/101/eclass/notes/file.txt", 3); err != nil {
		t.Fatal(err)
	}
}
