package rdbms

import (
	"errors"
	"fmt"

	"tree-eclass/internal/domain/database"
)

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
