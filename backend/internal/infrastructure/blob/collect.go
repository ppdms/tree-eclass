package blob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
)

// ownedName admits only our own content-addressed files. Foreign names and
// subdirectories inside the objects root are never collected.
var ownedName = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Sweep struct{ Versions, Bytes int64 }
type Registered func(context.Context, []string, []string) ([]bool, error)

// PruneUnregistered removes only stored files that the catalog no longer
// references. The caller must keep all publishers stopped for the entire
// operation. Foreign names and subdirectories are never touched.
func (s *Store) PruneUnregistered(ctx context.Context, registered Registered) (Sweep, error) {
	var result Sweep
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return result, err
	}
	keys, versions, sizes := collect(entries)
	if len(keys) == 0 {
		return result, nil
	}
	keep, err := registered(ctx, keys, versions)
	if err != nil {
		return result, err
	}
	if len(keep) != len(keys) {
		return result, errors.New("incomplete object reference verification")
	}
	for i, retain := range keep {
		if retain {
			continue
		}
		if err = s.removeVerified(keys[i], versions[i], sizes[i]); err != nil {
			return result, err
		}
		result.Versions++
		result.Bytes += sizes[i]
	}
	return result, nil
}

func collect(entries []os.DirEntry) ([]string, []string, []int64) {
	keys, versions, sizes := []string{}, []string{}, []int64{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !ownedName.MatchString(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.Size() < 0 {
			continue
		}
		keys, versions, sizes = append(keys, "objects/"+name), append(versions, name), append(sizes, info.Size())
	}
	return keys, versions, sizes
}

// removeVerified deletes one object file and confirms the deletion took
// effect, mirroring the acknowledged-deletion guarantee of the previous store.
func (s *Store) removeVerified(key, version string, size int64) error {
	path := s.path(version)
	if err := os.Remove(path); err != nil {
		return err
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"object deletion was not acknowledged; retry collection (key=%s version=%s size=%d)",
			key,
			version,
			size,
		)
	}
	return nil
}
