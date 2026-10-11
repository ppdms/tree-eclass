package rdbms_test

import (
	"database/sql"
	"os"
	"testing"

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
}

func TestEmptyConfigCreatesNoStorage(t *testing.T) {
	dir := t.TempDir()
	cfg := rdbms.Config{}
	store, err := rdbms.Open(t.Context(), cfg)
	if store != nil {
		store.Close()
	}
	if err == nil {
		t.Fatal("empty configuration was silently accepted")
	}
	if err := rdbms.Migrate(t.Context(), cfg); err == nil {
		t.Fatal("empty configuration was silently migrated")
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("empty configuration touched storage before rejection: %v", entries)
	}
}
