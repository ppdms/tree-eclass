// Package rdbms implements the SQL-free database ports over SQLite.
// Driver handles, SQL text and scanners are private to this infrastructure layer.
package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

// Config carries the SQLite database path. Empty Config is invalid.
type Config struct {
	// SQLitePath is a filesystem path to the sqlite database file.
	SQLitePath string
}

// Open connects SQLite and returns typed operation ports.
func Open(ctx context.Context, cfg Config) (database.Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return openSQLite(ctx, cfg)
}

func (cfg Config) validate() error {
	if cfg.SQLitePath == "" {
		return errors.New("rdbms: sqlite path is required")
	}
	return nil
}

// Migrate applies the sqlite schema chain. It is separate
// from Open so the controller can migrate while writers are stopped, before
// any pool (and its ownership lock) exists.
func Migrate(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	return migrateSQLite(ctx, cfg.SQLitePath)
}

// Require validates that the connected database matches the embedded schema
// chain exactly. Open calls it after acquiring ownership.
func Require(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	return requireSQLite(ctx, cfg.SQLitePath)
}

// Manifest returns the embedded migration name->sha256 map.
// Release packaging compares manifests across binaries for compatibility.
func Manifest() (map[string]string, error) {
	return sqliteManifest()
}
