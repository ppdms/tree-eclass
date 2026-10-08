// Package rdbms owns the relational-database boundary of the application.
//
// Domain, services and infrastructure code MUST program against the Pool, Tx
// and Rows interfaces here, never against a driver package. The two supported
// drivers live behind this seam: postgres (github.com/jackc/pgx/v5) and sqlite
// (modernc.org/sqlite). Selecting a driver is construction only: Open selects
// by DSN scheme, and every connector keeps working unchanged afterwards.
//
// Placeholder discipline: callers always write postgres-style $N placeholders.
// The sqlite driver rewrites them to ? at Exec/Query time, so the same SQL
// text runs on both backends. Statements that need other dialect rewrites
// (FOR UPDATE/SHARE, advisory locks) go through the helpers in dialect.go.
package rdbms

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrNoRows reports that a query expected one row and found none. It is the
// driver-neutral alias every not-found check MUST use instead of a driver
// sentinel such as pgx.ErrNoRows.
var ErrNoRows = errors.New("rdbms: no rows in result set")

// Isolation selects the transaction isolation level. SQLite serializes writers,
// so RepeatableRead and Serializable both map to an immediate transaction.
type Isolation int

const (
	// Default lets the driver pick its natural level (read committed on
	// postgres, serialized on sqlite).
	Default Isolation = iota
	// RepeatableRead backs the long snapshot reads used across the codebase.
	RepeatableRead
	// Serializable is reserved for future writers that need it.
	Serializable
)

// AccessMode selects read-only or read-write transactions.
type AccessMode int

const (
	// ReadWrite is the default for writers.
	ReadWrite AccessMode = iota
	// ReadOnly backs the snapshot reads; sqlite enforces nothing and relies on
	// the caller issuing no writes.
	ReadOnly
)

// Options configures Begin/BeginTx. The zero value is a read-write transaction
// at the driver default isolation.
type Options struct {
	Isolation  Isolation
	AccessMode AccessMode
}

// Rows streams a query result. It mirrors the pgx.Rows iteration contract the
// codebase already uses: Next/Scan/Err with an explicit Close.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// Row is a single-row query result.
type Row interface {
	Scan(dest ...any) error
}

// Result reports the rows affected by an Exec.
type Result interface {
	RowsAffected() int64
}

// DBTX is the query surface shared by pools and transactions. It intentionally
// mirrors the sqlc-generated DBTX shape (Exec/Query/QueryRow) so generated
// code keeps compiling unchanged on top of either driver.
type DBTX interface {
	Exec(ctx context.Context, query string, args ...any) (Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Tx is a database transaction: the query surface plus commit/rollback.
type Tx interface {
	DBTX
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Pool is a connection pool or single backing database. Begin starts a
// read-write transaction at the driver default isolation; BeginTx honors the
// requested options as far as the driver allows.
type Pool interface {
	DBTX
	Begin(ctx context.Context) (Tx, error)
	BeginTx(ctx context.Context, opts Options) (Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// Config selects the driver and its connection parameters. Exactly one of
// PostgresURL and SQLitePath is set; empty Config is invalid.
type Config struct {
	// PostgresURL is a postgresql:// URL, unchanged from current usage.
	PostgresURL string
	// SQLitePath is a filesystem path to the sqlite database file.
	SQLitePath string
	// MaxConns bounds the postgres pool. SQLite ignores it: modernc sqlite
	// serializes writers, so the driver keeps a single connection.
	MaxConns int32
}

// Open selects the driver by Config and returns a ready Pool. Postgres keeps
// the current behavior (pool + ownership lock + schema validation); sqlite
// opens (creating parent directories) and runs the sqlite migration chain.
func Open(ctx context.Context, cfg Config) (Pool, error) {
	switch {
	case cfg.PostgresURL != "":
		return openPostgres(ctx, cfg)
	case cfg.SQLitePath != "":
		return openSQLite(ctx, cfg)
	default:
		return nil, errors.New("rdbms: no database configured")
	}
}

// Migrate applies the schema chain for the configured driver. It is separate
// from Open so the controller can migrate while writers are stopped, before
// any pool (and its ownership lock) exists.
func Migrate(ctx context.Context, cfg Config) error {
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

// NowUTC returns the canonical timestamp text the schema stores (UTC,
// YYYY-MM-DD HH24:MI:SS). Callers that currently inline
// to_char(clock_timestamp() ...) MUST use this on the sqlite path; the
// postgres path keeps its SQL-side defaults untouched.
func NowUTC() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

// MapError converts a driver error to its neutral equivalent: sql.ErrNoRows
// becomes ErrNoRows, anything else passes through unchanged.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNoRows
	}
	return err
}
