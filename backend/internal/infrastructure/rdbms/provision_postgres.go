package rdbms

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/database"
)

// EnsurePostgresDatabase provisions only the named local lifecycle database.
// Connection failure is ErrUnavailable so the controller retains its readiness
// deadline; a connected schema/provisioning failure is not a retryable wait.
func EnsurePostgresDatabase(ctx context.Context, url, name string) error {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return err
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("%w: %s", database.ErrUnavailable, err)
	}
	defer conn.Close(context.Background())
	var exists bool
	if err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil {
		return mapPostgresError(err)
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	return mapPostgresError(err)
}
