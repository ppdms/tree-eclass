package rdbms

import (
	"context"
	"database/sql"

	"tree-eclass/internal/domain/database"
)

// WrapSQLite injects an already-open sqlite handle into typed repositories.
// Fixtures own admission and lifecycle: the handle stays open across WrapSQLite
// calls and the fixture closes it. Normal runtimes use Open, which acquires
// ownership and migrates. Transactions run through the same pool machinery
// as production; Close is a no-op because the fixture owns the handle.
func WrapSQLite(db *sql.DB) database.Store {
	pool := &sqlitePool{db: db, writer: make(chan struct{}, 1)}
	pool.writer <- struct{}{}
	return &sqliteFixture{conn: &sqliteConn{db: db}, pool: pool}
}

// sqliteFixture binds every operation accessor to one fixture-owned handle.
type sqliteFixture struct {
	conn *sqliteConn
	pool *sqlitePool
}

func (f *sqliteFixture) Begin(ctx context.Context) (database.Tx, error) {
	return f.pool.Begin(ctx)
}

func (f *sqliteFixture) BeginTx(ctx context.Context, opts database.Options) (database.Tx, error) {
	return f.pool.BeginTx(ctx, opts)
}

func (f *sqliteFixture) Ping(ctx context.Context) error {
	return mapSQLiteError(f.conn.db.PingContext(ctx))
}

func (f *sqliteFixture) Close() {
	// Drain admitted transactions without closing the fixture-owned handle.
	f.pool.life.close(func() {})
}

// sqliteConn adapts a fixture-owned *sql.DB to the native handle contract.
type sqliteConn struct{ db *sql.DB }

func (c *sqliteConn) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	n, err := result.RowsAffected()
	return sqliteResult{n: n}, mapSQLiteError(err)
}

func (c *sqliteConn) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	return &sqliteRows{rows: rows}, nil
}

func (c *sqliteConn) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	return &sqliteRow{row: c.db.QueryRowContext(ctx, query, args...)}
}

func (f *sqliteFixture) Courses() database.Courses {
	return f.conn.Courses()
}

func (f *sqliteFixture) Activity() database.Activity {
	return f.conn.Activity()
}

func (f *sqliteFixture) Exercises() database.Exercises {
	return f.conn.Exercises()
}

func (f *sqliteFixture) Settings() database.Settings {
	return f.conn.Settings()
}

func (f *sqliteFixture) Navigation() database.Navigation {
	return f.conn.Navigation()
}

func (f *sqliteFixture) Study() database.Study {
	return f.conn.Study()
}

func (f *sqliteFixture) Workspace() database.Workspace {
	return f.conn.Workspace()
}

func (f *sqliteFixture) Annotations() database.Annotations {
	return f.conn.Annotations()
}

func (f *sqliteFixture) Practice() database.Practice {
	return f.conn.Practice()
}

func (f *sqliteFixture) Documents() database.Documents {
	return f.conn.Documents()
}

func (f *sqliteFixture) Indexing() database.Indexing {
	return f.conn.Indexing()
}

func (f *sqliteFixture) Community() database.Community {
	return f.conn.Community()
}

func (f *sqliteFixture) DiscordImports() database.DiscordImports {
	return f.conn.DiscordImports()
}

func (f *sqliteFixture) Materials() database.Materials {
	return f.conn.Materials()
}

func (f *sqliteFixture) Objects() database.Objects {
	return f.conn.Objects()
}

func (f *sqliteFixture) Analysis() database.Analysis {
	return f.conn.Analysis()
}

func (f *sqliteFixture) Synthesis() database.Synthesis {
	return f.conn.Synthesis()
}

func (f *sqliteFixture) Sync() database.Sync {
	return f.conn.Sync()
}

func (f *sqliteFixture) Chat() database.Chat {
	return f.conn.Chat()
}

func (f *sqliteFixture) Discord() database.Discord {
	return f.conn.Discord()
}

func (f *sqliteFixture) PDFDifferences() database.PDFDifferences {
	return f.conn.PDFDifferences()
}

func (f *sqliteFixture) Notifications() database.Notifications {
	return f.conn.Notifications()
}

func (f *sqliteFixture) Quota() database.Quota {
	return f.conn.Quota()
}

func (f *sqliteFixture) Jobs() database.Jobs {
	return f.conn.Jobs()
}

func (f *sqliteFixture) ObjectCatalog() database.ObjectCatalog {
	return f.conn.ObjectCatalog()
}

func (f *sqliteFixture) Runtime() database.Runtime {
	return f.conn.Runtime()
}
