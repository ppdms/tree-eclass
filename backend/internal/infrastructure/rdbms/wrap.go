package rdbms

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/database"
)

// WrapPostgres injects an already-admitted native pool into typed repositories.
// Disposable fixtures own admission and lifecycle; normal runtimes use Open.
func WrapPostgres(pool *pgxpool.Pool) database.Store {
	return &postgresPool{pool: pool}
}

// WrapConn binds operations to an already-owned connection, used by offline
// lifecycle fixtures. The returned contract never exposes its SQL handle.
func WrapConn(conn *pgx.Conn) database.Operations {
	return &postgresConn{conn: conn}
}

type postgresConn struct{ conn *pgx.Conn }

func (c *postgresConn) Exec(ctx context.Context, query string, args ...any) (nativeResult, error) {
	tag, err := c.conn.Exec(ctx, query, args...)
	return postgresResult{tag: tag}, mapPostgresError(err)
}

func (c *postgresConn) Query(ctx context.Context, query string, args ...any) (nativeRows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows}, nil
}

func (c *postgresConn) QueryRow(ctx context.Context, query string, args ...any) nativeRow {
	return &postgresRow{row: c.conn.QueryRow(ctx, query, args...)}
}
