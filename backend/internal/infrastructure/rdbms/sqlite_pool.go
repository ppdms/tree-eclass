package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
)

func (p *sqlitePool) admitWriter(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.writer:
		return func() { p.writer <- struct{}{} }, nil
	}
}

func (p *sqlitePool) writeGuard(ctx context.Context, query string) (func(), error) {
	if isWriteStatement(query) {
		return p.admitWriter(ctx)
	}
	return nil, nil
}

// Classification controls pool-level writer admission, never SQL rewriting.
// Conservatively lock WITH queries, whose final statement may mutate data.
func isWriteStatement(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	for _, prefix := range []string{"SELECT ", "SELECT(", "EXPLAIN ", "PRAGMA ", "VALUES("} {
		if strings.HasPrefix(upper, prefix) {
			return false
		}
	}
	return true
}

func (p *sqlitePool) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	defer p.life.leave()
	release, err := p.writeGuard(ctx, query)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	result, err := p.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	n, err := result.RowsAffected()
	return sqliteResult{n: n}, mapSQLiteError(err)
}

func (p *sqlitePool) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	release, err := p.operationGuard(ctx, query)
	if err != nil {
		p.life.leave()
		return nil, err
	}
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, mapSQLiteError(err)
	}
	return &sqliteRows{rows: rows, release: release}, nil
}

func (p *sqlitePool) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	if err := p.life.enter(); err != nil {
		return &sqliteRow{err: err}
	}
	release, err := p.operationGuard(ctx, query)
	if err != nil {
		p.life.leave()
		return &sqliteRow{err: err}
	}
	return &sqliteRow{row: p.db.QueryRowContext(ctx, query, args...), release: release}
}

func (p *sqlitePool) Begin(ctx context.Context) (database.Tx, error) {
	return p.BeginTx(ctx, database.Options{})
}

func (p *sqlitePool) BeginTx(ctx context.Context, opts database.Options) (database.Tx, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	t := &sqliteTx{pool: p, readOnly: opts.AccessMode == database.ReadOnly, admitted: true}
	var err error
	if !t.readOnly {
		if t.release, err = p.admitWriter(ctx); err != nil {
			t.cleanup()
			return nil, err
		}
	}
	if t.conn, err = p.db.Conn(ctx); err != nil {
		t.cleanup()
		return nil, mapSQLiteError(err)
	}
	statement := "BEGIN IMMEDIATE"
	if t.readOnly {
		statement = "BEGIN"
		_, err = t.conn.ExecContext(ctx, "PRAGMA query_only=ON")
	}
	if err == nil {
		_, err = t.conn.ExecContext(ctx, statement)
	}
	if err != nil {
		t.cleanup()
		return nil, mapSQLiteError(err)
	}
	return t, nil
}

func (p *sqlitePool) Ping(ctx context.Context) error {
	if err := p.life.enter(); err != nil {
		return err
	}
	defer p.life.leave()
	if err := checkSQLiteOwner(p.owner, p.path); err != nil {
		return err
	}
	return mapSQLiteError(p.db.PingContext(ctx))
}

func (p *sqlitePool) Close() {
	p.life.close(func() {
		_ = p.db.Close()
		_ = p.owner.Close()
	})
}

func (p *sqlitePool) operationGuard(ctx context.Context, query string) (func(), error) {
	writer, err := p.writeGuard(ctx, query)
	if err != nil {
		return nil, err
	}
	return func() {
		if writer != nil {
			writer()
		}
		p.life.leave()
	}, nil
}
