package rdbms

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsNoRows reports whether err is a not-found condition on either driver.
func IsNoRows(err error) bool {
	return errors.Is(err, ErrNoRows) || errors.Is(err, pgx.ErrNoRows)
}

// IsUniqueViolation reports whether err is a unique-constraint violation on
// either driver. Postgres surfaces pgconn.PgError code 23505; sqlite surfaces
// SQLITE_CONSTRAINT_UNIQUE, matched by message in the sqlite driver (mapped
// before reaching here) or passed through as constraint text.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	var uniq *uniqueError
	return errors.As(err, &uniq)
}

// mapPostgresError converts pgx.ErrNoRows to the neutral ErrNoRows so callers
// can check one sentinel. All other errors pass through unchanged, preserving
// errors.As access to *pgconn.PgError for constraint mapping.
func mapPostgresError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}
	return err
}
