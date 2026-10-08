package rdbms

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/database"
)

type postgresPool struct {
	pool       *pgxpool.Pool
	owner      *pgxpool.Conn
	ownerProbe sync.Mutex
	life       storeLifetime
}

type postgresTx struct {
	tx     pgx.Tx
	pool   *postgresPool
	done   bool
	failed bool
}
type postgresRows struct {
	rows    pgx.Rows
	release func()
	onError func(error)
	once    sync.Once
}
type postgresRow struct {
	row     pgx.Row
	err     error
	release func()
	onError func(error)
}
type postgresResult struct{ tag pgconn.CommandTag }

func openPostgres(ctx context.Context, cfg Config) (database.Store, error) {
	parsed, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, err
	}
	parsed.MinConns = 0
	parsed.MaxConns = cfg.MaxConns
	if parsed.MaxConns <= 0 {
		parsed.MaxConns = 6
	}
	parsed.MaxConnIdleTime = 30 * time.Second
	parsed.ConnConfig.RuntimeParams["search_path"] = "public"
	parsed.ConnConfig.RuntimeParams["application_name"] = "tree-eclass"
	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		return nil, err
	}
	p := &postgresPool{pool: pool}
	if err = p.admit(ctx, cfg); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *postgresPool) admit(ctx context.Context, cfg Config) error {
	owner, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	p.owner = owner
	var locked bool
	if err = owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", runtimeLock).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return errors.New("another application owns this database")
	}
	return requirePostgres(ctx, cfg.PostgresURL)
}

func (p *postgresPool) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	defer p.life.leave()
	tag, err := p.pool.Exec(ctx, query, args...)
	return postgresResult{tag: tag}, mapPostgresError(err)
}

func (p *postgresPool) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		p.life.leave()
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows, release: p.life.leave}, nil
}

func (p *postgresPool) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	if err := p.life.enter(); err != nil {
		return &postgresRow{err: err}
	}
	return &postgresRow{row: p.pool.QueryRow(ctx, query, args...), release: p.life.leave}
}

func (p *postgresPool) Begin(ctx context.Context) (database.Tx, error) {
	return p.BeginTx(ctx, database.Options{})
}

func (p *postgresPool) BeginTx(ctx context.Context, opts database.Options) (database.Tx, error) {
	if err := p.life.enter(); err != nil {
		return nil, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgxIso(opts.Isolation), AccessMode: pgxAccess(opts.AccessMode),
	})
	if err != nil {
		p.life.leave()
		return nil, mapPostgresError(err)
	}
	return &postgresTx{tx: tx, pool: p}, nil
}

func (p *postgresPool) Ping(ctx context.Context) error {
	if err := p.life.enter(); err != nil {
		return err
	}
	defer p.life.leave()
	if p.owner != nil {
		// Health requests and the ownership monitor share this non-concurrent connection.
		p.ownerProbe.Lock()
		defer p.ownerProbe.Unlock()
		return mapPostgresError(p.owner.Ping(ctx))
	}
	return mapPostgresError(p.pool.Ping(ctx))
}

func (p *postgresPool) Close() {
	p.life.close(func() {
		if p.owner != nil {
			_ = p.owner.Conn().Close(context.Background())
			p.owner.Release()
		}
		p.pool.Close()
	})
}

func pgxIso(level database.Isolation) pgx.TxIsoLevel {
	switch level {
	case database.RepeatableRead:
		return pgx.RepeatableRead
	case database.Serializable:
		return pgx.Serializable
	default:
		return pgx.ReadCommitted
	}
}

func pgxAccess(mode database.AccessMode) pgx.TxAccessMode {
	if mode == database.ReadOnly {
		return pgx.ReadOnly
	}
	return pgx.ReadWrite
}

func (t *postgresTx) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	if err := t.status(); err != nil {
		return nil, err
	}
	tag, err := t.tx.Exec(ctx, query, args...)
	t.markFailure(err)
	return postgresResult{tag: tag}, mapPostgresError(err)
}

func (t *postgresTx) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	if err := t.status(); err != nil {
		return nil, err
	}
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		t.markFailure(err)
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows, onError: t.markFailure}, nil
}

func (t *postgresTx) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	if err := t.status(); err != nil {
		return &postgresRow{err: err}
	}
	return &postgresRow{row: t.tx.QueryRow(ctx, query, args...), onError: t.markFailure}
}

func (t *postgresTx) Commit(ctx context.Context) error {
	if t.done {
		return database.ErrFinished
	}
	t.done = true
	defer t.pool.life.leave()
	if t.failed {
		_ = t.tx.Rollback(context.WithoutCancel(ctx))
		return database.ErrFailed
	}
	return mapPostgresError(t.tx.Commit(ctx))
}
func (t *postgresTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.pool.life.leave()
	err := t.tx.Rollback(context.WithoutCancel(ctx))
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return mapPostgresError(err)
}
func (r *postgresRows) Next() bool {
	if !r.rows.Next() {
		r.Close()
		return false
	}
	return true
}
func (r *postgresRows) Scan(dest ...any) error {
	err := r.rows.Scan(dest...)
	if r.onError != nil {
		r.onError(err)
	}
	return mapPostgresError(err)
}
func (r *postgresRows) Err() error {
	err := r.rows.Err()
	if r.onError != nil {
		r.onError(err)
	}
	return mapPostgresError(err)
}
func (r *postgresRows) Close() {
	r.once.Do(func() {
		r.rows.Close()
		if r.onError != nil {
			r.onError(r.rows.Err())
		}
		if r.release != nil {
			r.release()
		}
	})
}
func (r *postgresRow) Scan(dest ...any) error {
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
	return mapPostgresError(err)
}
func (r postgresResult) RowsAffected() int64 { return r.tag.RowsAffected() }

func (t *postgresTx) status() error {
	if t.done {
		return database.ErrFinished
	}
	if t.failed {
		return database.ErrFailed
	}
	return nil
}

func (t *postgresTx) markFailure(err error) {
	var failure *pgconn.PgError
	if errors.As(err, &failure) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.failed = true
	}
}
