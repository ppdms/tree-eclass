package rdbms

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// The runtime and migrator share this kernel ownership lock. A stale lock file
// is harmless; the kernel releases its lock when its owning process exits.
func acquireSQLiteOwner(path string) (*os.File, error) {
	owner, err := os.OpenFile(path+".owner", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = owner.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errors.New("another application or migration owns this database")
		}
		return nil, err
	}
	return owner, nil
}

func checkSQLiteOwner(owner *os.File, path string) error {
	current, err := os.Stat(path + ".owner")
	if err != nil {
		return fmt.Errorf("sqlite ownership lost: %w", err)
	}
	locked, err := owner.Stat()
	if err != nil {
		return fmt.Errorf("sqlite ownership lost: %w", err)
	}
	if !os.SameFile(current, locked) {
		return errors.New("sqlite ownership file replaced")
	}
	return nil
}
