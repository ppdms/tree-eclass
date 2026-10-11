package rdbms

import (
	"context"
	"errors"
)

// Backend implementations acquire semantic transaction locks here. The key is
// documentation: the admitted IMMEDIATE writer owns every write until
// commit/rollback, so a write transaction already holds the lock.
func advisoryLock(ctx context.Context, db nativeDBTX, _ string, _, _ bool) (bool, error) {
	switch tx := db.(type) {
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
