package blob

import (
	"context"
	"fmt"
	"os"
)

// Check verifies the local storage contract without network access: the
// objects root must be a directory that accepts and releases a probe file.
func (s *Store) Check(ctx context.Context) error {
	info, err := os.Stat(s.root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("objects root %s is not a directory", s.root)
	}
	probe, err := os.CreateTemp(s.root, "probe-*")
	if err != nil {
		return err
	}
	name := probe.Name()
	if err = probe.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Remove(name)
}
