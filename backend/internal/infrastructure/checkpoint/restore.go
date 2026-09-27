package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
)

func (s Store) journalPath() string { return filepath.Join(s.Root, "restore.json") }
func (s Store) journal(j restoreJournal, phase string) error {
	j.Phase = phase
	if err := platform.WriteJSON(s.journalPath(), j); err != nil {
		return err
	}
	return s.step(phase)
}

func (s Store) Restore(id string) error {
	if err := s.stopped(); err != nil {
		return err
	}
	if _, err := s.Read(id); err != nil {
		return err
	}
	var j restoreJournal
	if err := platform.ReadJSON(s.journalPath(), &j); err == nil {
		if j.Snapshot != id {
			return fmt.Errorf("finish interrupted restore of %s first", j.Snapshot)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else {
		for _, name := range []string{".restore-old", ".restore-ready"} {
			if exists(filepath.Join(s.Root, name)) {
				return fmt.Errorf("unregistered restore directory %s; refusing overwrite", name)
			}
		}
		if err = s.journal(restoreJournal{Snapshot: id}, "prepare"); err != nil {
			return err
		}
	}
	return s.Recover()
}

// Recover is restartable after a crash before or after any filesystem rename.
// The old active data remains until a complete replacement has been published.
func (s Store) Recover() error {
	var j restoreJournal
	if err := platform.ReadJSON(s.journalPath(), &j); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := s.stopped(); err != nil {
		return err
	}
	if _, err := s.Read(j.Snapshot); err != nil {
		return err
	}
	source, _ := s.path(j.Snapshot)
	prepared, retired := filepath.Join(s.Root, ".restore-ready"), filepath.Join(s.Root, ".restore-old")
	var err error
	if j, err = s.prepareRestore(j, source, prepared); err != nil {
		return err
	}
	if j, err = s.retireActive(j, retired); err != nil {
		return err
	}
	if j, err = s.promotePrepared(j, prepared); err != nil {
		return err
	}
	if j.Phase != "swapped" {
		return fmt.Errorf("unknown restore phase %q", j.Phase)
	}
	if err := os.RemoveAll(retired); err != nil {
		return err
	}
	if err := os.Remove(s.journalPath()); err != nil {
		return err
	}
	return platform.SyncDir(s.Root)
}

func (s Store) prepareRestore(j restoreJournal, source, prepared string) (restoreJournal, error) {
	if j.Phase != "prepare" {
		return j, nil
	}
	if err := os.RemoveAll(prepared); err != nil {
		return j, err
	}
	if err := platform.CloneTree(filepath.Join(source, "data"), prepared); err != nil {
		return j, err
	}
	if err := s.journal(j, "ready"); err != nil {
		return j, err
	}
	j.Phase = "ready"
	return j, nil
}

func (s Store) retireActive(j restoreJournal, retired string) (restoreJournal, error) {
	if j.Phase != "ready" {
		return j, nil
	}
	if !exists(retired) {
		if err := os.Rename(s.Active(), retired); err != nil {
			return j, err
		}
		if err := platform.SyncDir(s.Root); err != nil {
			return j, err
		}
	}
	if err := s.journal(j, "retired"); err != nil {
		return j, err
	}
	j.Phase = "retired"
	return j, nil
}

func (s Store) promotePrepared(j restoreJournal, prepared string) (restoreJournal, error) {
	if j.Phase != "retired" {
		return j, nil
	}
	if !exists(s.Active()) {
		if err := os.Rename(prepared, s.Active()); err != nil {
			return j, err
		}
		if err := platform.SyncDir(s.Root); err != nil {
			return j, err
		}
	}
	if err := s.journal(j, "swapped"); err != nil {
		return j, err
	}
	j.Phase = "swapped"
	return j, nil
}

func exists(path string) bool { _, err := os.Lstat(path); return err == nil }
