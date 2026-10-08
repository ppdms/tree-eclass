// Package storage owns database connections and SQL migration compatibility.
//
// Open selects a backend implementation whose typed operations satisfy domain
// persistence ports. SQL, driver handles and result decoding stay in rdbms.
package storage

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Config selects the database backend. It aliases rdbms.Config so callers
// configure one type: DatabaseURL for postgres, SQLitePath for sqlite.
type Config = rdbms.Config

// Database owns an admitted backend and exposes SQL-free operation ports.
type Database struct {
	Pool database.Store
}

// ConfigForURL builds a postgres Config from a postgresql:// URL, preserving
// every existing call site that passes cfg.DatabaseURL.
func ConfigForURL(url string) Config {
	return rdbms.Config{PostgresURL: url}
}

// Open connects and admits the database (ownership lock + schema validation).
func Open(ctx context.Context, url string) (*Database, error) {
	return OpenConfig(ctx, ConfigForURL(url))
}

// OpenConfig connects with an explicit driver selection.
func OpenConfig(ctx context.Context, cfg Config) (*Database, error) {
	pool, err := rdbms.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Database{Pool: pool}, nil
}

// Close releases ownership and the pool.
func (d *Database) Close() {
	if d == nil || d.Pool == nil {
		return
	}
	d.Pool.Close()
}

// CheckOwner probes the admitted backend's exclusive ownership. PostgreSQL
// probes its dedicated session; SQLite verifies its locked file identity.
func (d *Database) CheckOwner(ctx context.Context) error {
	return d.Pool.Ping(ctx)
}

// Migrate applies the schema chain for the URL-selected driver while writers
// are stopped.
func Migrate(ctx context.Context, url string) error {
	return MigrateConfig(ctx, ConfigForURL(url))
}

// MigrateConfig applies the schema chain for the configured driver.
func MigrateConfig(ctx context.Context, cfg Config) error {
	return rdbms.Migrate(ctx, cfg)
}

// Require validates the schema ledger exactly.
func Require(ctx context.Context, url string) error {
	return rdbms.Require(ctx, ConfigForURL(url))
}

// Manifest returns the migration name->sha256 map for release compatibility.
func Manifest() (map[string]string, error) {
	return rdbms.Manifest("postgres")
}

// ManifestFor returns the manifest for either driver.
func ManifestFor(driver string) (map[string]string, error) {
	return rdbms.Manifest(driver)
}
