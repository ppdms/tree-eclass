package rdbms_test

import (
	"errors"
	"reflect"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/rdbms"
)

func TestTypedCourseContract(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			service := courses.Service{Pool: store}
			for _, id := range []int64{30, 10, 20} {
				if err := service.Add(t.Context(), id, "Ελληνικά\x00μάθημα", "Σ\x00"); err != nil {
					t.Fatal(err)
				}
			}
			assertCourses(t, service, false, []int64{10, 20, 30})
			if err := service.Reorder(t.Context(), []int64{30, 10, 20}); err != nil {
				t.Fatal(err)
			}
			assertCourses(t, service, false, []int64{30, 10, 20})
			if err := service.Add(t.Context(), 40, "Ελληνικά\x00μάθημα", "Σ\x00"); err != nil {
				t.Fatal(err)
			}
			if err := store.Courses().OrderCourse(t.Context(), database.OrderCourseParams{ID: 40}); err != nil {
				t.Fatal(err)
			}
			assertCourses(t, service, false, []int64{30, 10, 20, 40})
			assertShelfOrder(t, store, []int64{30, 10, 20, 40})
			if err := service.Hide(t.Context(), 10, true); err != nil {
				t.Fatal(err)
			}
			assertCourses(t, service, false, []int64{30, 20, 40})
			assertCourses(t, service, true, []int64{30, 10, 20, 40})
			if _, err := service.Get(t.Context(), 999); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("missing course error = %v", err)
			}
			if err := service.Add(t.Context(), 30, "duplicate"); !errors.Is(err, database.ErrUnique) {
				t.Fatalf("duplicate course error = %v", err)
			}
		})
	}
}

func assertCourses(t *testing.T, service courses.Service, hidden bool, expected []int64) {
	t.Helper()
	items, err := service.List(t.Context(), hidden)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
		if item.Name != "Ελληνικά\x00μάθημα" || item.ShortName == nil || *item.ShortName != "Σ\x00" {
			t.Fatalf("reversible course identity lost: %+v", item)
		}
	}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("course visibility/order = %v; want %v", ids, expected)
	}
}

func TestCrossFeatureTransactionAndRunningCoalescing(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			tx, err := store.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if err = tx.Courses().AddCourse(t.Context(), database.AddCourseParams{
				ID: 701, Name: "rolled back", WebdavFolder: "/Courses/701",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err = jobs.EnqueueTx(t.Context(), tx, "contract", "rebuild", nil, true); err != nil {
				t.Fatal(err)
			}
			if err = tx.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err = store.Courses().Course(t.Context(), 701); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("rolled-back course visible: %v", err)
			}
			queue := jobs.Queue{Pool: store}
			claimed, err := queue.Claim(t.Context(), "contract")
			if err != nil || len(claimed) != 0 {
				t.Fatalf("rolled-back work admitted: %+v, %v", claimed, err)
			}
			assertRunningCoalescing(t, queue)
		})
	}
}

func assertRunningCoalescing(t *testing.T, queue jobs.Queue) {
	t.Helper()
	first, err := queue.Enqueue(t.Context(), "contract", "rebuild", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := queue.Claim(t.Context(), "contract")
	if err != nil || len(claimed) != 1 || claimed[0].ID != first || claimed[0].Status != "running" {
		t.Fatalf("claim did not capture pending command: %+v, %v", claimed, err)
	}
	second, err := queue.Enqueue(t.Context(), "contract", "rebuild", nil, true)
	if err != nil || second == first {
		t.Fatalf("running inputs swallowed later change: %q, %v", second, err)
	}
	coalesced, err := queue.Enqueue(t.Context(), "contract", "rebuild", nil, true)
	if err != nil || coalesced != second {
		t.Fatalf("pending successor not coalesced: %q, %v", coalesced, err)
	}
	if err = queue.Complete(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	claimed, err = queue.Claim(t.Context(), "contract")
	if err != nil || len(claimed) != 1 || claimed[0].ID != second {
		t.Fatalf("completion lost pending successor: %+v, %v", claimed, err)
	}
}

func TestReadOnlySnapshotAndConnectionReuse(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			service := courses.Service{Pool: store}
			if err := service.Add(t.Context(), 11, "baseline"); err != nil {
				t.Fatal(err)
			}
			tx, err := store.BeginTx(t.Context(), database.Options{
				Isolation: database.RepeatableRead, AccessMode: database.ReadOnly,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			before, err := tx.Courses().Course(t.Context(), 11)
			if err != nil {
				t.Fatal(err)
			}
			if err = service.Rename(t.Context(), 11, "committed successor"); err != nil {
				t.Fatal(err)
			}
			after, err := tx.Courses().Course(t.Context(), 11)
			if err != nil || after.Name != before.Name {
				t.Fatalf("snapshot changed across writer commit: %+v, %v", after, err)
			}
			assertReadOnlyAndReuse(t, store, tx)
		})
	}
}

func assertReadOnlyAndReuse(t *testing.T, store database.Store, tx database.Tx) {
	t.Helper()
	_, err := tx.Courses().RenameCourse(t.Context(), database.RenameCourseParams{ID: 11, Name: "illegal"})
	if !errors.Is(err, database.ErrReadOnly) {
		t.Fatalf("read-only write error = %v", err)
	}
	if err = tx.Commit(t.Context()); !errors.Is(err, database.ErrFailed) {
		t.Fatalf("failed transaction committed: %v", err)
	}
	service := courses.Service{Pool: store}
	if err = service.Rename(t.Context(), 11, "connection reusable"); err != nil {
		t.Fatalf("read-only connection leaked into a writer: %v", err)
	}
	item, err := service.Get(t.Context(), 11)
	if err != nil || item.Name != "connection reusable" {
		t.Fatalf("successor write not persisted: %+v, %v", item, err)
	}
}

func TestRuntimeAndMigrationOwnershipContract(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if competing, err := rdbms.Open(t.Context(), backend.cfg); err == nil {
				competing.Close()
				t.Fatal("two runtimes admitted to one dataset")
			}
			if err := rdbms.Migrate(t.Context(), backend.cfg); err == nil {
				t.Fatal("migration admitted against active runtime")
			}
			store.Close()
			if err := rdbms.Migrate(t.Context(), backend.cfg); err != nil {
				t.Fatalf("cold migration rejected: %v", err)
			}
			_ = openContractStore(t, backend.cfg)
		})
	}
}

func assertShelfOrder(t *testing.T, store database.Store, expected []int64) {
	t.Helper()
	rows, err := store.Courses().ShelfRows(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Course.ID)
	}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("shelf course order = %v, want %v", ids, expected)
	}
}
