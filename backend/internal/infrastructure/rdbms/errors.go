package rdbms

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"tree-eclass/internal/domain/database"
)

func mapPostgresError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return database.ErrNoRows
	}
	if errors.Is(err, pgx.ErrTxClosed) {
		return database.ErrFinished
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return database.ErrFailed
	}
	var failure *pgconn.PgError
	if !errors.As(err, &failure) {
		return err
	}
	switch failure.Code {
	case "23505":
		return fmt.Errorf("%w: %s", database.ErrUnique, failure.Message)
	case "23503":
		return fmt.Errorf("%w: %s", database.ErrForeignKey, failure.Message)
	case "25006":
		return fmt.Errorf("%w: %s", database.ErrReadOnly, failure.Message)
	case "25P02":
		return database.ErrFailed
	default:
		return errors.New("database: " + failure.Message)
	}
}

func mapSQLiteConstraint(err error) error {
	if err == nil {
		return nil
	}
	var failure interface{ Code() int }
	if !errors.As(err, &failure) {
		return err
	}
	switch failure.Code() {
	case 1555, 2067:
		return fmt.Errorf("%w: %s", database.ErrUnique, err)
	case 787:
		return fmt.Errorf("%w: %s", database.ErrForeignKey, err)
	}
	if failure.Code()&0xff == 8 {
		return fmt.Errorf("%w: %s", database.ErrReadOnly, err)
	}
	return errors.New("database: " + err.Error())
}
