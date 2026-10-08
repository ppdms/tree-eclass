package rdbms

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestSQLiteMigrationsApply executes every embedded sqlite migration file
// statement-by-statement against an in-memory database. It catches dialect
// errors and splitter regressions (e.g. trigger bodies split mid-BEGIN).
func TestSQLiteMigrationsApply(t *testing.T) {
	ms, err := readMigrations(sqliteFS, "sqlite_migrations")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 24 {
		t.Fatalf("expected 24 sqlite migrations, got %d", len(ms))
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	for _, m := range ms {
		for _, stmt := range splitStatements(m.SQL) {
			if isCommentOnly(stmt) {
				continue
			}
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("%s: %v\nstatement: %.200s", m.Name, err, stmt)
			}
		}
	}
	var tables, triggers, indexes int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='trigger'").Scan(&triggers); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='index' AND sql IS NOT NULL").Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	t.Logf("tables=%d triggers=%d indexes=%d", tables, triggers, indexes)
	if tables < 60 {
		t.Fatalf("too few tables applied: %d", tables)
	}
	if triggers < 30 {
		t.Fatalf("too few triggers applied: %d", triggers)
	}
}

// TestSQLiteMigrateEndToEnd runs the full Migrate+Require chain on a temp
// file, proving the ledger contract works outside memory.
func TestSQLiteMigrateEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tree-test.db")
	ctx := context.Background()
	if err := Migrate(ctx, Config{SQLitePath: path}); err != nil {
		t.Fatal(err)
	}
	if err := Require(ctx, Config{SQLitePath: path}); err != nil {
		t.Fatal(err)
	}
	// Second migrate must be idempotent.
	if err := Migrate(ctx, Config{SQLitePath: path}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
