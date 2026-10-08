package rdbms

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WrapPostgres adapts a native *pgxpool.Pool to the Pool interface. Test
// fixtures keep building raw pgx pools; wrapping at the fixture boundary
// keeps production code driver-neutral while tests stay postgres-native.
func WrapPostgres(pool *pgxpool.Pool) Pool {
	return &postgresPool{pool: pool}
}

// connDBTX adapts a *pgx.Conn to the DBTX query surface for test fixtures
// that hold single connections.
type connDBTX struct {
	conn *pgx.Conn
}

func (c *connDBTX) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	tag, err := c.conn.Exec(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return postgresResult{tag: tag}, nil
}

func (c *connDBTX) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return &postgresRows{rows: rows}, nil
}

func (c *connDBTX) QueryRow(ctx context.Context, query string, args ...any) Row {
	return &postgresRow{row: c.conn.QueryRow(ctx, query, args...)}
}

// WrapConn adapts a native *pgx.Conn to rdbms.DBTX. Test fixtures using
// single connections (cold_restore, collect) use this at the call site.
func WrapConn(conn *pgx.Conn) DBTX {
	return &connDBTX{conn: conn}
}
