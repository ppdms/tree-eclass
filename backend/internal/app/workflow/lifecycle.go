package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/checkpoint"
)

// Up always selects a fixed release and restores an interrupted development
// session before it can launch. This is the explicit discard-on-exit contract.
func (c *Controller) Up(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if c.State.Mode == "stable" && c.Processes.Alive("api") {
		return nil
	}
	if c.State.Release == "" {
		return errors.New("no stable release selected; build and use a clean committed release first")
	}
	if _, err := c.release(c.State.Release); err != nil {
		return err
	}
	if err := c.Down(); err != nil {
		return err
	}
	if err := c.ensureProviderKeys(ctx); err != nil {
		return err
	}
	c.State.Mode = "stable"
	c.State.Session = checkpoint.ID()
	c.State.Started = time.Now().UTC()
	if err := c.save(); err != nil {
		return err
	}
	return c.launch(ctx)
}

func (c *Controller) DevUp(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if c.State.Mode == "development" && c.State.Baseline != "" && c.Processes.Alive("api") &&
		c.Processes.Alive("frontend-build") &&
		c.Processes.Alive("watcher") {
		return nil
	}
	if err := c.stopAll(); err != nil {
		return err
	}
	if err := c.snapshots().Recover(); err != nil {
		return err
	}
	if c.State.Baseline == "" {
		if err := c.ensureProviderKeys(ctx); err != nil {
			return err
		}
		if err := c.space(); err != nil {
			return err
		}
		m, err := c.snapshots().
			Create(checkpoint.Manifest{Release: c.State.Release, Reason: "development", Versions: c.versions()})
		if err != nil {
			return err
		}
		c.State.Baseline = m.ID
	} else {
		if err := c.space(); err != nil {
			return err
		}
		if _, err := c.snapshots().Create(checkpoint.Manifest{
			Release:     c.State.Release,
			Reason:      "development-resume",
			Development: c.State.Baseline,
			Versions:    c.versions(),
		}); err != nil {
			return err
		}
	}
	c.State.Mode = "development"
	c.State.Session = checkpoint.ID()
	c.State.Started = time.Now().UTC()
	if err := c.save(); err != nil {
		return err
	}
	fmt.Printf("Development baseline %s protected. All development data will be discarded on exit.\n", c.State.Baseline)
	return c.launch(ctx)
}

func (c *Controller) launch(ctx context.Context) (err error) {
	// A failed start leaves storage stopped and preserves any development baseline.
	defer func() {
		if err != nil {
			err = errors.Join(err, c.stopAll())
		}
	}()
	if err = c.infrastructure(ctx); err != nil {
		return err
	}
	if err = c.application(ctx); err != nil {
		return err
	}
	if c.State.Mode == "development" {
		if err = c.start(ctx, "watcher", []string{c.Executable, "_watch", c.Root, c.Repo, c.State.Session}, nil, 0); err != nil {
			return err
		}
	}
	fmt.Printf("%s is ready at http://127.0.0.1:%d\n", c.State.Mode, c.Config.Ports.HTTP)
	return nil
}

func (c *Controller) Down() error {
	if err := c.recoverActivation(); err != nil {
		return err
	}
	if err := c.configured(); err != nil {
		return err
	}
	if err := c.stopAll(); err != nil {
		return err
	}
	if err := c.snapshots().Recover(); err != nil {
		return err
	}
	if c.State.Baseline != "" {
		fmt.Printf("Discarding development data; restoring stable checkpoint %s.\n", c.State.Baseline)
		if err := c.snapshots().Restore(c.State.Baseline); err != nil {
			return err
		}
		c.State.Baseline = ""
	}
	c.State.Mode = "stopped"
	c.State.Session = checkpoint.ID()
	if err := c.save(); err != nil {
		return err
	}
	if err := c.cleanBuild(); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Repo, "frontend/dist")); err != nil {
		return err
	}
	return c.prune()
}

func (c *Controller) versions() map[string]string {
	return map[string]string{"postgres": c.Config.PostgresVersion, "seaweedfs": c.Config.WeedVersion}
}

func (c *Controller) SnapshotRestore(id string) error {
	c.pin = id
	defer func() { c.pin = "" }()
	m, err := c.snapshots().Read(id)
	if err != nil {
		return err
	}
	if m.Development != "" {
		return errors.New(
			"development checkpoints cannot replace stable data; exit development to restore its baseline",
		)
	}
	if m.Versions["postgres"] != c.Config.PostgresVersion || m.Versions["seaweedfs"] != c.Config.WeedVersion {
		return errors.New("checkpoint requires different infrastructure binaries")
	}
	if err = c.Down(); err != nil {
		return err
	}
	if err = c.space(); err != nil {
		return err
	}
	rescue, err := c.snapshots().
		Create(checkpoint.Manifest{Release: c.State.Release, Reason: "before-restore", Versions: c.versions()})
	if err != nil {
		return err
	}
	fmt.Printf("Restoring %s: later stable writes will be discarded. Current state protected in %s.\n", id, rescue.ID)
	if err = c.snapshots().Restore(id); err != nil {
		return err
	}
	c.State.Release = m.Release
	c.State.Activation = rescue.ID
	c.State.Mode = "stopped"
	return c.save()
}

func (c *Controller) SnapshotList() error {
	snapshots, err := c.snapshots().List()
	if err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		fmt.Printf("%s  %s  release=%s\n", snapshot.ID, snapshot.Reason, snapshot.Release)
	}
	return nil
}

func (c *Controller) prune() error {
	protected := map[string]bool{c.State.Baseline: true, c.State.Activation: true, c.pin: true}
	snapshots, err := c.snapshots().List()
	if err != nil {
		return err
	}
	// Retain one most recent completed checkpoint besides explicitly protected ones.
	for i := len(snapshots) - 1; i >= 0; i-- {
		latest := snapshots[i]
		if latest.Development == "" || latest.Development == c.State.Baseline {
			protected[latest.ID] = true
			break
		}
	}
	retained := []checkpoint.Manifest{}
	for _, snapshot := range snapshots {
		if protected[snapshot.ID] && (snapshot.Development == "" || snapshot.Development == c.State.Baseline) {
			retained = append(retained, snapshot)
			continue
		}
		if err = os.RemoveAll(filepath.Join(c.Root, "checkpoints", snapshot.ID)); err != nil {
			return err
		}
	}
	if err = c.pruneReleases(retained); err != nil {
		return err
	}
	return platform.SyncDir(c.Root)
}
