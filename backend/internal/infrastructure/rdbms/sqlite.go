package rdbms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// sqlitePool is the sqlite driver behind the Pool interface. WAL mode lets
// readers proceed while one writer holds the database; the pool sizes to a
// few connections so a long crawl or projection build cannot starve health
// probes and claimers at pool level. Writer transactions serialize on
// writeMu: sqlite cannot upgrade a DEFERRED read snapshot once another
// connection commits (SQLITE_BUSY_SNAPSHOT), so at most one writer tx is
// open at a time and upgrades never race. The process-local advisory-lock
// table keeps transaction-scoped lock semantics.
type sqlitePool struct {
	db      *sql.DB
	mu      sync.Mutex
	locks   map[string]*sync.Mutex
	writeMu sync.Mutex
}

type sqliteTx struct {
	tx     *sql.Tx
	pool   *sqlitePool
	held   []*sync.Mutex
	done   bool
	writer bool
}

type sqliteRows struct {
	rows *sql.Rows
}

type sqliteRow struct {
	row *sql.Row
	err error
}

type sqliteResult struct {
	n int64
}

func openSQLite(ctx context.Context, cfg Config) (Pool, error) {
	if cfg.SQLitePath == "" {
		return nil, errors.New("rdbms: sqlite path is empty")
	}
	if err := ensureParentDir(cfg.SQLitePath); err != nil {
		return nil, err
	}
	// busy_timeout lets a contender wait out a long crawl or projection
	// build (tens of seconds) instead of failing immediately; WAL mode keeps
	// pure readers unblocked.
	dsn := "file:" + cfg.SQLitePath + "?_pragma=busy_timeout%3D30000&_pragma=journal_mode%3DWAL&_pragma=foreign_keys%3D1"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	p := &sqlitePool{db: db, locks: map[string]*sync.Mutex{}}
	if err = migrateSQLite(ctx, cfg.SQLitePath); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err = requireSQLite(ctx, cfg.SQLitePath); err != nil {
		_ = db.Close()
		return nil, err
	}
	return p, nil
}

// lockFor returns the process-local mutex for an advisory key, creating it on
// first use. Advisory locks are transaction-scoped: the sqlite transaction
// holds the mutex from first lock statement until commit/rollback.
func (p *sqlitePool) lockFor(key string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.locks[key]
	if !ok {
		m = &sync.Mutex{}
		p.locks[key] = m
	}
	return m
}

func (p *sqlitePool) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		m := p.lockFor(lock.key)
		m.Lock()
		defer m.Unlock()
		return sqliteResult{}, nil
	}
	// Pool-level writes commit immediately; serializing them with writer
	// transactions keeps DEFERRED snapshots upgradeable (SQLITE_BUSY_SNAPSHOT
	// fires when any other connection commits first).
	if isWriteStatement(translated) {
		p.writeMu.Lock()
		defer p.writeMu.Unlock()
	}
	res, err := p.db.ExecContext(ctx, translated, expanded...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	n, _ := res.RowsAffected()
	return sqliteResult{n: n}, nil
}

// isWriteStatement reports statements that can commit: anything but a bare
// SELECT/EXPLAIN/PRAGMA/VALUES read. WITH takes the lock (it may wrap a
// write); misclassifying a read as a write only costs contention.
func isWriteStatement(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	for _, prefix := range []string{"SELECT ", "SELECT(", "EXPLAIN ", "PRAGMA ", "VALUES("} {
		if strings.HasPrefix(upper, prefix) {
			return false
		}
	}
	return true
}

func (p *sqlitePool) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		m := p.lockFor(lock.key)
		m.Lock()
		defer m.Unlock()
		rows, err := p.db.QueryContext(ctx, "SELECT 1 WHERE 0", args...)
		if err != nil {
			return nil, mapSQLiteError(err)
		}
		return &sqliteRows{rows: rows}, nil
	}
	rows, err := p.db.QueryContext(ctx, translated, expanded...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	return &sqliteRows{rows: rows}, nil
}

func (p *sqlitePool) QueryRow(ctx context.Context, query string, args ...any) Row {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return &sqliteRow{err: err}
	}
	if lock != nil {
		// pg_try_advisory_xact_lock returns a boolean; report success.
		var held bool = true
		_ = held
		return &sqliteRow{err: nil, row: p.db.QueryRowContext(ctx, "SELECT 1")}
	}
	return &sqliteRow{row: p.db.QueryRowContext(ctx, translated, expanded...)}
}

func (p *sqlitePool) Begin(ctx context.Context) (Tx, error) {
	return p.BeginTx(ctx, Options{})
}

func (p *sqlitePool) BeginTx(ctx context.Context, opts Options) (Tx, error) {
	// BEGIN DEFERRED takes no lock: readers proceed under WAL and the write
	// lock is acquired only if the transaction actually writes. Eager
	// IMMEDIATE would serialize every snapshot read behind writers and
	// starve probes on a small pool. Read-only transactions never touch
	// writeMu; writers hold it from begin to finish so sqlite never faces
	// a concurrent-commit upgrade (SQLITE_BUSY_SNAPSHOT).
	writer := opts.AccessMode != ReadOnly
	if writer {
		p.writeMu.Lock()
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		if writer {
			p.writeMu.Unlock()
		}
		return nil, mapSQLiteError(err)
	}
	return &sqliteTx{tx: tx, pool: p, writer: writer}, nil
}

func (p *sqlitePool) Ping(ctx context.Context) error {
	return mapSQLiteError(p.db.PingContext(ctx))
}

func (p *sqlitePool) Close() { _ = p.db.Close() }

func (t *sqliteTx) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		m := t.pool.lockFor(lock.key)
		m.Lock()
		t.held = append(t.held, m)
		return sqliteResult{}, nil
	}
	res, err := t.tx.ExecContext(ctx, translated, expanded...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	n, _ := res.RowsAffected()
	return sqliteResult{n: n}, nil
}

func (t *sqliteTx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		m := t.pool.lockFor(lock.key)
		m.Lock()
		t.held = append(t.held, m)
		rows, err := t.tx.QueryContext(ctx, "SELECT 1 WHERE 0")
		if err != nil {
			return nil, mapSQLiteError(err)
		}
		return &sqliteRows{rows: rows}, nil
	}
	rows, err := t.tx.QueryContext(ctx, translated, expanded...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	return &sqliteRows{rows: rows}, nil
}

func (t *sqliteTx) QueryRow(ctx context.Context, query string, args ...any) Row {
	translated, expanded, lock, err := translate(query, args)
	if err != nil {
		return &sqliteRow{err: err}
	}
	if lock != nil {
		m := t.pool.lockFor(lock.key)
		m.Lock()
		t.held = append(t.held, m)
		return &sqliteRow{row: t.tx.QueryRowContext(ctx, "SELECT 1")}
	}
	return &sqliteRow{row: t.tx.QueryRowContext(ctx, translated, expanded...)}
}

func (t *sqliteTx) releaseLocks() {
	for i := len(t.held) - 1; i >= 0; i-- {
		t.held[i].Unlock()
	}
	t.held = nil
	if t.writer {
		t.writer = false
		t.pool.writeMu.Unlock()
	}
}

func (t *sqliteTx) Commit(ctx context.Context) error {
	if t.done {
		return errors.New("rdbms: transaction already finished")
	}
	t.done = true
	defer t.releaseLocks()
	return mapSQLiteError(t.tx.Commit())
}

func (t *sqliteTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.releaseLocks()
	_ = t.tx.Rollback()
	return nil
}

func (r *sqliteRows) Next() bool { return r.rows.Next() }

func (r *sqliteRows) Scan(dest ...any) error {
	return mapSQLiteError(r.rows.Scan(dest...))
}

func (r *sqliteRows) Err() error { return mapSQLiteError(r.rows.Err()) }

func (r *sqliteRows) Close() { _ = r.rows.Close() }

func (r *sqliteRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return mapSQLiteError(r.row.Scan(dest...))
}

func (r sqliteResult) RowsAffected() int64 { return r.n }

// Batch executes a statement list inside the caller's transaction. It
// replaces pgx.Batch+SendBatch at migrated call sites: same atomicity, no
// driver type in the signature.
func Batch(ctx context.Context, tx Tx, statements []string, args [][]any) error {
	if len(statements) != len(args) {
		return fmt.Errorf("rdbms: batch shape mismatch %d statements vs %d arg sets", len(statements), len(args))
	}
	for i, stmt := range statements {
		if _, err := tx.Exec(ctx, stmt, args[i]...); err != nil {
			return err
		}
	}
	return nil
}

// mapSQLiteError converts database/sql no-rows and sqlite constraint text to
// neutral sentinels. Unique violations surface as SQLITE_CONSTRAINT_UNIQUE in
// modernc errors; anything else passes through.
func mapSQLiteError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNoRows
	}
	msg := err.Error()
	if strings.Contains(msg, "SQLITE_CONSTRAINT_UNIQUE") || strings.Contains(msg, "UNIQUE constraint failed") {
		return &uniqueError{msg: msg}
	}
	return err
}

type uniqueError struct{ msg string }

func (e *uniqueError) Error() string { return "rdbms: unique violation: " + e.msg }
