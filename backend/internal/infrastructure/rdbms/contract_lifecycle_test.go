package rdbms_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/rdbms"
)

func TestCanceledStatementCannotCommitEarlierWrites(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			tx, err := store.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if err := tx.Courses().AddCourse(t.Context(), database.AddCourseParams{
				ID: 88, Name: "must roll back", WebdavFolder: "/Courses/88",
			}); err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := tx.Courses().Course(canceled, 88); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled statement error = %v", err)
			}
			if err := tx.Commit(t.Context()); !errors.Is(err, database.ErrFailed) {
				t.Fatalf("canceled transaction committed: %v", err)
			}
			if _, err := store.Courses().Course(t.Context(), 88); !errors.Is(err, database.ErrNoRows) {
				t.Fatalf("earlier transaction write survived cancellation: %v", err)
			}
		})
	}
}

func TestCloseRetainsOwnershipUntilAdmittedTransactionEnds(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			tx, err := store.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			closed := make(chan struct{})
			go func() { store.Close(); close(closed) }()
			waitClosedAdmission(t, store)
			competitor, err := rdbms.Open(t.Context(), backend.cfg)
			if competitor != nil {
				competitor.Close()
			}
			if err == nil {
				t.Fatal("ownership released while an admitted transaction was live")
			}
			if err := tx.Courses().AddCourse(t.Context(), database.AddCourseParams{
				ID: 89, Name: "admitted writer", WebdavFolder: "/Courses/89",
			}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not complete after the final transaction")
			}
			reopened := openContractStore(t, backend.cfg)
			if row, err := reopened.Courses().Course(t.Context(), 89); err != nil || row.Name != "admitted writer" {
				t.Fatalf("admitted transaction was not preserved: %+v, %v", row, err)
			}
		})
	}
}

func waitClosedAdmission(t *testing.T, store database.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		err := store.Ping(ctx)
		if errors.Is(err, database.ErrFinished) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("Close did not stop new operation admission")
		}
		runtime.Gosched()
	}
}

func TestSQLiteAliasesShareRuntimeAndMigrationOwnership(t *testing.T) {
	cfg := rdbms.Config{SQLitePath: filepath.Join(t.TempDir(), "database.db")}
	store := openContractStore(t, cfg)
	alias := filepath.Join(filepath.Dir(cfg.SQLitePath), "alias.db")
	if err := os.Symlink(cfg.SQLitePath, alias); err != nil {
		t.Fatal(err)
	}
	aliased := rdbms.Config{SQLitePath: alias}
	competitor, err := rdbms.Open(t.Context(), aliased)
	if competitor != nil {
		competitor.Close()
	}
	if err == nil {
		t.Fatal("database symlink admitted a second runtime")
	}
	if err := rdbms.Migrate(t.Context(), aliased); err == nil {
		t.Fatal("database symlink bypassed runtime/migration exclusion")
	}
	store.Close()
	reopened := openContractStore(t, aliased)
	if err := reopened.Courses().AddCourse(t.Context(), database.AddCourseParams{
		ID: 90, Name: "aliased", WebdavFolder: "/Courses/90",
	}); err != nil {
		t.Fatal(err)
	}
}
