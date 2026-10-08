// Package rdbms implements the SQL-free database ports using native backend SQL.
// Driver handles, SQL text and scanners are private to this infrastructure layer.
package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

// Config selects the driver and its connection parameters. Exactly one of
// PostgresURL and SQLitePath is set; empty Config is invalid.
type Config struct {
	// PostgresURL is a postgresql:// URL, unchanged from current usage.
	PostgresURL string
	// SQLitePath is a filesystem path to the sqlite database file.
	SQLitePath string
	// MaxConns bounds the PostgreSQL pool; SQLite keeps bounded WAL readers
	// and one context-cancelable writer admission slot.
	MaxConns int32
}

// Open selects exactly one backend and returns typed operation ports.
func Open(ctx context.Context, cfg Config) (database.Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.PostgresURL != "" {
		return openPostgres(ctx, cfg)
	}
	return openSQLite(ctx, cfg)
}

func (cfg Config) validate() error {
	if (cfg.PostgresURL == "") == (cfg.SQLitePath == "") {
		return errors.New("rdbms: configure exactly one of postgres URL and sqlite path")
	}
	return nil
}

// Migrate applies the schema chain for the configured driver. It is separate
// from Open so the controller can migrate while writers are stopped, before
// any pool (and its ownership lock) exists.
func Migrate(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	switch {
	case cfg.PostgresURL != "":
		return migratePostgres(ctx, cfg.PostgresURL)
	case cfg.SQLitePath != "":
		return migrateSQLite(ctx, cfg.SQLitePath)
	default:
		return errors.New("rdbms: no database configured")
	}
}

// Require validates that the connected database matches the embedded schema
// chain exactly. Open calls it after acquiring ownership.
func Require(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	switch {
	case cfg.PostgresURL != "":
		return requirePostgres(ctx, cfg.PostgresURL)
	case cfg.SQLitePath != "":
		return requireSQLite(ctx, cfg.SQLitePath)
	default:
		return errors.New("rdbms: no database configured")
	}
}

// Manifest returns the embedded migration name->sha256 map for the driver.
// Release packaging compares manifests across binaries for compatibility.
func Manifest(driver string) (map[string]string, error) {
	switch driver {
	case "postgres":
		return postgresManifest()
	case "sqlite":
		return sqliteManifest()
	default:
		return nil, errors.New("rdbms: unknown driver " + driver)
	}
}

// Driver reports which backend backs a Config: "postgres" or "sqlite".
func Driver(cfg Config) string {
	if cfg.SQLitePath != "" {
		return "sqlite"
	}
	return "postgres"
}
