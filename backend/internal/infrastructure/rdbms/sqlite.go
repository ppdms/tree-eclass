package rdbms

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"sync"

	_ "modernc.org/sqlite"

	"tree-eclass/internal/domain/database"
)

type sqlitePool struct {
	db     *sql.DB
	writer chan struct{}
	owner  *os.File
	path   string
	life   storeLifetime
}

type sqliteTx struct {
	conn     *sql.Conn
	pool     *sqlitePool
	release  func()
	readOnly bool
	done     bool
	failed   bool
	admitted bool
}

type sqliteRows struct {
	rows    *sql.Rows
	release func()
	once    sync.Once
	onError func(error)
}

type sqliteRow struct {
	row     *sql.Row
	err     error
	release func()
	onError func(error)
}

type sqliteResult struct{ n int64 }

func openSQLite(ctx context.Context, cfg Config) (database.Store, error) {
	path, err := canonicalSQLitePath(cfg.SQLitePath)
	if err != nil {
		return nil, err
	}
	cfg.SQLitePath = path
	owner, err := acquireSQLiteOwner(cfg.SQLitePath)
	if err != nil {
		return nil, err
	}
	if err = migrateSQLiteOwned(ctx, cfg.SQLitePath); err != nil {
		_ = owner.Close()
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(cfg.SQLitePath))
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	p := &sqlitePool{db: db, writer: make(chan struct{}, 1), owner: owner, path: cfg.SQLitePath}
	p.writer <- struct{}{}
	if err = p.Ping(ctx); err == nil {
		err = requireSQLite(ctx, cfg.SQLitePath)
	}
	if err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func sqliteDSN(path string) string {
	return "file:" + path + "?_pragma=busy_timeout%3D30000&_pragma=journal_mode%3DWAL&_pragma=foreign_keys%3D1"
}

func (t *sqliteTx) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	if t.done {
		return nil, database.ErrFinished
	}
	if t.failed {
		return nil, database.ErrFailed
	}
	result, err := t.conn.ExecContext(ctx, query, args...)
	if err != nil {
		t.markFailure(err)
		return nil, mapSQLiteError(err)
	}
	n, err := result.RowsAffected()
	return sqliteResult{n: n}, mapSQLiteError(err)
}

func (t *sqliteTx) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	if t.done {
		return nil, database.ErrFinished
	}
	if t.failed {
		return nil, database.ErrFailed
	}
	rows, err := t.conn.QueryContext(ctx, query, args...)
	if err != nil {
		t.markFailure(err)
		return nil, mapSQLiteError(err)
	}
	return &sqliteRows{rows: rows, onError: t.markFailure}, nil
}

func (t *sqliteTx) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	if t.done {
		return &sqliteRow{err: database.ErrFinished}
	}
	if t.failed {
		return &sqliteRow{err: database.ErrFailed}
	}
	return &sqliteRow{row: t.conn.QueryRowContext(ctx, query, args...), onError: t.markFailure}
}

func (t *sqliteTx) markFailure(err error) {
	var failure interface{ Code() int }
	if errors.As(err, &failure) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.failed = true
	}
}

func (t *sqliteTx) Commit(ctx context.Context) error {
	if t.done {
		return database.ErrFinished
	}
	t.done = true
	defer t.cleanup()
	if t.failed {
		_, _ = t.conn.ExecContext(context.Background(), "ROLLBACK")
		return database.ErrFailed
	}
	_, err := t.conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		_, _ = t.conn.ExecContext(context.Background(), "ROLLBACK")
	}
	return mapSQLiteError(err)
}

func (t *sqliteTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.cleanup()
	_, err := t.conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	return mapSQLiteError(err)
}

func (t *sqliteTx) cleanup() {
	if t.conn != nil {
		if t.readOnly {
			if _, err := t.conn.ExecContext(context.Background(), "PRAGMA query_only=OFF"); err != nil {
				_ = t.conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
		_ = t.conn.Close()
	}
	if t.release != nil {
		t.release()
		t.release = nil
	}
	if t.admitted {
		t.pool.life.leave()
		t.admitted = false
	}
}

func (r *sqliteRows) Next() bool {
	if !r.rows.Next() {
		if r.onError != nil {
			r.onError(r.rows.Err())
		}
		r.Close()
		return false
	}
	return true
}
func (r *sqliteRows) Scan(dest ...any) error {
	err := r.rows.Scan(dest...)
	if r.onError != nil {
		r.onError(err)
	}
	return mapSQLiteError(err)
}
func (r *sqliteRows) Err() error {
	err := r.rows.Err()
	if r.onError != nil {
		r.onError(err)
	}
	return mapSQLiteError(err)
}
func (r *sqliteRows) Close() {
	r.once.Do(func() {
		_ = r.rows.Close()
		if r.onError != nil {
			r.onError(r.rows.Err())
		}
		if r.release != nil {
			r.release()
		}
	})
}
func (r *sqliteRow) Scan(dest ...any) error {
	if r.release != nil {
		defer r.release()
		r.release = nil
	}
	if r.err != nil {
		return r.err
	}
	err := r.row.Scan(dest...)
	if r.onError != nil {
		r.onError(err)
	}
	return mapSQLiteError(err)
}
func (r sqliteResult) RowsAffected() int64 { return r.n }

func mapSQLiteError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return database.ErrNoRows
	}
	return mapSQLiteConstraint(err)
}
