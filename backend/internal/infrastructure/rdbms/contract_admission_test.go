package rdbms_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/infrastructure/rdbms"
)

func TestChangedAppliedMigrationIsNeverRepairedOrAdmitted(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			if err := rdbms.Migrate(t.Context(), backend.cfg); err != nil {
				t.Fatal(err)
			}
			corruptMigrationLedger(t, backend)
			if err := rdbms.Require(t.Context(), backend.cfg); err == nil {
				t.Fatal("changed applied migration admitted by schema check")
			}
			if err := rdbms.Migrate(t.Context(), backend.cfg); err == nil {
				t.Fatal("changed applied migration silently repaired")
			}
			store, err := rdbms.Open(t.Context(), backend.cfg)
			if store != nil {
				store.Close()
			}
			if err == nil {
				t.Fatal("application admitted an incompatible migration ledger")
			}
		})
	}
}

func corruptMigrationLedger(t *testing.T, backend contractBackend) {
	t.Helper()
	if backend.name == "sqlite" {
		db, err := sql.Open("sqlite", backend.cfg.SQLitePath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		_, err = db.ExecContext(t.Context(), `UPDATE tree_go_migrations SET sha256='invalid'
			WHERE version=(SELECT min(version) FROM tree_go_migrations)`)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	conn, err := pgx.Connect(t.Context(), backend.cfg.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	if _, err := conn.Exec(t.Context(), `UPDATE public.tree_go_migrations SET sha256='invalid'
		WHERE version=(SELECT min(version) FROM public.tree_go_migrations)`); err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousBackendSelectionDoesNotCreateStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ambiguous", "database.db")
	cfg := rdbms.Config{PostgresURL: "postgresql://invalid/unused", SQLitePath: path}
	store, err := rdbms.Open(t.Context(), cfg)
	if store != nil {
		store.Close()
	}
	if err == nil {
		t.Fatal("both configured backends were silently accepted")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("ambiguous configuration touched storage before rejection: %v", err)
	}
}
