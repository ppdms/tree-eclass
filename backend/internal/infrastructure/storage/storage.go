// Package storage owns the sqlite database connection and SQL migration compatibility.
//
// OpenConfig connects the backend whose typed operations satisfy domain
// persistence ports. SQL, driver handles and result decoding stay in rdbms.
package storage

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Config carries the sqlite database path. It aliases rdbms.Config so callers
// configure one type.
type Config = rdbms.Config

// Database owns an admitted backend and exposes SQL-free operation ports.
type Database struct {
	Pool database.Store
}

// OpenConfig connects and admits the database (ownership lock + schema validation).
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

// CheckOwner probes the admitted backend's exclusive ownership by verifying
// its locked file identity.
func (d *Database) CheckOwner(ctx context.Context) error {
	return d.Pool.Ping(ctx)
}

// MigrateConfig applies the schema chain while writers are stopped.
func MigrateConfig(ctx context.Context, cfg Config) error {
	return rdbms.Migrate(ctx, cfg)
}

// Require validates the sqlite schema ledger exactly.
func Require(ctx context.Context, cfg Config) error {
	return rdbms.Require(ctx, cfg)
}

// Manifest returns the migration name->sha256 map for release compatibility.
func Manifest() (map[string]string, error) {
	return rdbms.Manifest()
}
