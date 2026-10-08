package rdbms

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// postgresPool adapts pgxpool.Pool to the Pool interface. It is the default
// driver: identical behavior to the pre-abstraction storage.Database pool.
type postgresPool struct {
	pool  *pgxpool.Pool
	owner *pgxpool.Conn
}

type postgresTx struct {
	tx pgx.Tx
}

type postgresRows struct {
	rows pgx.Rows
}

type postgresRow struct {
	row pgx.Row
}

type postgresResult struct {
	tag pgconn.CommandTag
}

func openPostgres(ctx context.Context, cfg Config) (Pool, error) {
	parsed, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, err
	}
	maxConns := cfg.MaxConns
	if maxConns <= 0 {
		maxConns = 6
	}
	parsed.MinConns = 0
	parsed.MaxConns = maxConns
	parsed.MaxConnIdleTime = 30 * time.Second
	parsed.ConnConfig.RuntimeParams["search_path"] = "public"
	parsed.ConnConfig.RuntimeParams["application_name"] = "tree-eclass"
	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		return nil, err
	}
	owner, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	var ok bool
	if err = owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", runtimeLock).Scan(&ok); err != nil || !ok {
		owner.Release()
		pool.Close()
		if err == nil {
			err = errors.New("another application owns this database")
		}
		return nil, err
	}
	p := &postgresPool{pool: pool}
	p.owner = owner
	// Validate under the same ownership lock used by migrations, closing the
	// check-then-acquire race between application admission and schema changes.
	if err = requirePostgres(ctx, cfg.PostgresURL); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// owner is the dedicated physical connection holding the session advisory
// lock. Pool queries could succeed after that connection (and its exclusive
// session lock) is lost, so ownership probes MUST stay on owner.
func (p *postgresPool) checkOwner(ctx context.Context) error {
	return p.owner.Ping(ctx)
}

func (p *postgresPool) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	tag, err := p.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return postgresResult{tag: tag}, nil
}

func (p *postgresPool) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows}, nil
}

func (p *postgresPool) QueryRow(ctx context.Context, query string, args ...any) Row {
	return &postgresRow{row: p.pool.QueryRow(ctx, query, args...)}
}

func (p *postgresPool) Begin(ctx context.Context) (Tx, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresTx{tx: tx}, nil
}

func (p *postgresPool) BeginTx(ctx context.Context, opts Options) (Tx, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgxIso(opts.Isolation),
		AccessMode: pgxAccess(opts.AccessMode),
	})
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresTx{tx: tx}, nil
}

func (p *postgresPool) Ping(ctx context.Context) error {
	return mapPostgresError(p.checkOwner(ctx))
}

func (p *postgresPool) Close() {
	// Closing this physical connection releases its session lock even on
	// cancellation.
	_ = p.owner.Conn().Close(context.Background())
	p.owner.Release()
	p.pool.Close()
}

func pgxIso(level Isolation) pgx.TxIsoLevel {
	switch level {
	case RepeatableRead, Serializable:
		return pgx.RepeatableRead
	default:
		return pgx.ReadCommitted
	}
}

func pgxAccess(mode AccessMode) pgx.TxAccessMode {
	if mode == ReadOnly {
		return pgx.ReadOnly
	}
	return pgx.ReadWrite
}

func (t *postgresTx) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	tag, err := t.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return postgresResult{tag: tag}, nil
}

func (t *postgresTx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows}, nil
}

func (t *postgresTx) QueryRow(ctx context.Context, query string, args ...any) Row {
	return &postgresRow{row: t.tx.QueryRow(ctx, query, args...)}
}

func (t *postgresTx) Commit(ctx context.Context) error {
	return mapPostgresError(t.tx.Commit(ctx))
}

func (t *postgresTx) Rollback(ctx context.Context) error {
	return mapPostgresError(t.tx.Rollback(ctx))
}

func (r *postgresRows) Next() bool { return r.rows.Next() }

func (r *postgresRows) Scan(dest ...any) error {
	return mapPostgresError(r.rows.Scan(dest...))
}

func (r *postgresRows) Err() error { return mapPostgresError(r.rows.Err()) }

func (r *postgresRows) Close() { r.rows.Close() }

func (r *postgresRow) Scan(dest ...any) error {
	return mapPostgresError(r.row.Scan(dest...))
}

func (r postgresResult) RowsAffected() int64 { return r.tag.RowsAffected() }

// UnwrapPostgres exposes the underlying pool for code that has not migrated
// yet (sqlc-generated queries, CopyFrom/Batch call sites). New code MUST NOT
// use it; migrate the call site to Pool/Tx instead.
func UnwrapPostgres(p Pool) (*pgxpool.Pool, bool) {
	if pp, ok := p.(*postgresPool); ok {
		return pp.pool, true
	}
	return nil, false
}

// UnwrapPostgresTx exposes the underlying transaction for unmigrated code.
// New code MUST NOT use it.
func UnwrapPostgresTx(t Tx) (pgx.Tx, bool) {
	if pt, ok := t.(*postgresTx); ok {
		return pt.tx, true
	}
	return nil, false
}

// NativeDBTX is the pgx-native query surface the sqlc-generated code runs
// on. UnwrapDBTX exposes it for the queries package only; all other code
// MUST use Pool/Tx and never unwrap.
type NativeDBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// UnwrapDBTX exposes the native handle behind an rdbms handle: the raw pool
// or tx for postgres handles, ok=false for sqlite handles.
func UnwrapDBTX(db DBTX) (NativeDBTX, bool) {
	switch h := db.(type) {
	case *postgresPool:
		return h.pool, true
	case *postgresTx:
		return h.tx, true
	default:
		return nil, false
	}
}
