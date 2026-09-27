package workflow

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/checkpoint"
)

type latestBuild struct {
	Release string `json:"release"`
}

func (c *Controller) pruneReleases(snapshots []checkpoint.Manifest) error {
	root := filepath.Join(c.Root, "releases")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	protected := map[string]bool{c.State.Release: true, c.State.Previous: true, c.releasePin: true}
	for _, snapshot := range snapshots {
		protected[snapshot.Release] = true
	}
	var latest latestBuild
	err = platform.ReadJSON(filepath.Join(root, "latest-build.json"), &latest)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if latest.Release != "" {
		if !releaseID.MatchString(latest.Release) {
			return errors.New("invalid latest release marker")
		}
		protected[latest.Release] = true
	}
	removable := []string{}
	for _, entry := range entries {
		id := entry.Name()
		if !entry.IsDir() || !releaseID.MatchString(id) || protected[id] {
			continue
		}
		var manifest Release
		if err = platform.ReadJSON(filepath.Join(root, id, "release.json"), &manifest); err != nil {
			return fmt.Errorf("cannot inspect retained release %s: %w", id, err)
		}
		if manifest.ID != id || manifest.Commit != id {
			return fmt.Errorf("release retention identity mismatch: %s", id)
		}
		removable = append(removable, id)
	}
	// Finish inspection before any deletion. Unknown directories and linked roots
	// are never retention candidates; the controller owns only named manifests.
	for _, id := range removable {
		path := filepath.Join(root, id)
		if err = writableDirectories(path); err != nil {
			return err
		}
		if err = os.RemoveAll(path); err != nil {
			return err
		}
	}
	return platform.SyncDir(root)
}

func (c *Controller) recordBuilt(id string) error {
	if err := platform.WriteJSON(filepath.Join(c.Root, "releases/latest-build.json"), latestBuild{Release: id}); err != nil {
		return err
	}
	if err := c.prune(); err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}
