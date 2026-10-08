package rdbms

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Runtime and migration locks use the database's canonical path, not a config
// spelling that could alias the same dataset through a file/directory symlink.
func canonicalSQLitePath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if entry, statErr := os.Lstat(absolute); statErr == nil && entry.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("SQLite database symlink has no reachable target: %w", err)
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return "", statErr
	}
	if err := ensureParentDir(absolute); err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}
