package rdbms_test

import (
	"errors"
	"testing"

	"tree-eclass/internal/domain/database"
)

func TestForeignKeyFailureRollsBackCrossFeatureWrites(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			tx, err := store.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if err := tx.Courses().AddCourse(t.Context(), database.AddCourseParams{
				ID: 90, Name: "must roll back", WebdavFolder: "/Courses/90",
			}); err != nil {
				t.Fatal(err)
			}
			err = tx.Objects().RegisterRevision(t.Context(), database.RegisterRevisionParams{
				ID: "missing-object-revision", DocumentID: "missing-doc", CourseID: 90,
				LogicalPath: "/missing", ObjectID: "missing-object",
			})
			if !errors.Is(err, database.ErrForeignKey) {
				t.Fatalf("foreign-key error = %v", err)
			}
			if err := tx.Commit(t.Context()); !errors.Is(err, database.ErrFailed) {
				t.Fatalf("failed cross-feature transaction committed: %v", err)
			}
			if _, err := store.Courses().Course(t.Context(), 90); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("earlier write survived constraint failure: %v", err)
			}
		})
	}
}
