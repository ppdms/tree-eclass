package rdbms

import (
	"context"
	"errors"
)

// Backend implementations acquire semantic transaction locks here. PostgreSQL
// has keyed advisory locks; SQLite's admitted IMMEDIATE writer owns every write
// until commit/rollback, providing stronger exclusion without emulating SQL.
func advisoryLock(ctx context.Context, db nativeDBTX, key string, extended, try bool) (bool, error) {
	switch tx := db.(type) {
	case *postgresTx:
		expression := "hashtext($1)"
		if extended {
			expression = "hashtextextended($1,0)"
		}
		if try {
			var acquired bool
			err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock("+expression+")", key).Scan(&acquired)
			return acquired, err
		}
		_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock("+expression+")", key)
		return err == nil, err
	case *sqliteTx:
		if tx.done {
			return false, errors.New("database transaction already finished")
		}
		if tx.readOnly {
			return false, errors.New("transaction lock requires write access")
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, errors.New("transaction lock requires a transaction-bound repository")
	}
}
