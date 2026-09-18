// Package storage owns PostgreSQL connections and SQL migration compatibility.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const runtimeLock int64 = 87422102

type Database struct {
	Pool  *pgxpool.Pool
	owner *pgxpool.Conn
}

func Open(ctx context.Context, url string) (*Database, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MinConns = 0
	cfg.MaxConns = 6
	cfg.MaxConnIdleTime = 30 * time.Second
	cfg.ConnConfig.RuntimeParams["search_path"] = "public"
	cfg.ConnConfig.RuntimeParams["application_name"] = "tree-eclass"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	owner, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	var ok bool
	err = owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", runtimeLock).Scan(&ok)
	if err != nil || !ok {
		owner.Release()
		pool.Close()
		if err == nil {
			err = errors.New("another application owns this database")
		}
		return nil, err
	}
	d := &Database{pool, owner}
	// Validate under the same ownership lock used by migrations, closing the
	// check-then-acquire race between application admission and schema changes.
	if err = Require(ctx, url); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

func (d *Database) Close() {
	// Closing this physical connection releases its session lock even on cancellation.
	_ = d.owner.Conn().Close(context.Background())
	d.owner.Release()
	d.Pool.Close()
}

// CheckOwner must remain on the dedicated physical connection. Pool queries
// could succeed after that connection (and its exclusive session lock) is lost.
func (d *Database) CheckOwner(ctx context.Context) error {
	return d.owner.Ping(ctx)
}
