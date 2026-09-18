package workflow

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/platform"
)

type Release struct {
	ID         string            `json:"id"`
	Commit     string            `json:"commit"`
	Built      time.Time         `json:"built"`
	Migrations map[string]string `json:"migrations"`
	Files      map[string]string `json:"files"`
}

var releaseID = regexp.MustCompile(`^[0-9a-f]{40}$`)

func (c *Controller) release(id string) (Release, error) {
	var release Release
	if !releaseID.MatchString(id) {
		return release, errors.New("release ID must be a full committed Git SHA")
	}
	if err := platform.ReadJSON(filepath.Join(c.Root, "releases", id, "release.json"), &release); err != nil {
		return release, err
	}
	if release.ID != id || release.Commit != id {
		return release, errors.New("release identity mismatch")
	}
	return release, verifyRelease(filepath.Join(c.Root, "releases", id), release)
}

func (c *Controller) ReleaseBuild(ctx context.Context) (err error) {
	_, err = c.releaseBuild(ctx)
	return err
}

func (c *Controller) releaseBuild(ctx context.Context) (id string, err error) {
	if err := c.configured(); err != nil {
		return "", err
	}
	clean := c.releaseBuildCache()
	defer func() { err = errors.Join(err, clean()) }()
	id, err = c.buildRevision(ctx)
	if err != nil {
		return "", err
	}
	if _, err = c.release(id); err == nil {
		return id, c.recordBuilt(id)
	}
	return id, c.buildRelease(ctx, id)
}

// releasePromote builds the current clean commit, activates it, and starts
// stable mode. It stops the current mode only after packaging succeeds.
func (c *Controller) releasePromote(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if _, err := c.buildRevision(ctx); err != nil {
		return err
	}
	id, err := c.releaseBuild(ctx)
	if err != nil {
		return err
	}
	if err := c.Down(); err != nil {
		return err
	}
	return c.ReleaseUse(ctx, id)
}

// buildRevision refuses to build an uncommitted checkout and returns the HEAD SHA.
func (c *Controller) buildRevision(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "status", "--porcelain")
	cmd.Dir = c.Repo
	status, err := cmd.Output()
	if err != nil {
		return "", err
	}
	if len(status) > 0 {
		return "", errors.New(
			"stable releases require a clean committed checkout; commit the reviewed changes before building (tree will not commit for you)",
		)
	}
	cmd = exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = c.Repo
	revision, err := cmd.Output()
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(revision))
	if !releaseID.MatchString(id) {
		return "", errors.New("invalid source revision")
	}
	return id, nil
}

func (c *Controller) buildRelease(ctx context.Context, id string) (err error) {
	if err = c.space(); err != nil {
		return err
	}
	root := filepath.Join(c.Root, "releases")
	if err = os.MkdirAll(root, 0700); err != nil {
		return err
	}
	temp, err := os.MkdirTemp(root, ".build-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	if err = archiveSource(ctx, c.Repo, id, temp); err != nil {
		return err
	}
	release, err := c.buildArtifacts(ctx, temp, id)
	if err != nil {
		return err
	}
	if err = platform.WriteJSON(filepath.Join(temp, "release.json"), release); err != nil {
		return err
	}
	if err = platform.SyncTree(temp); err != nil {
		return err
	}
	if err = os.Rename(temp, filepath.Join(root, id)); err != nil {
		return err
	}
	if err = platform.SyncDir(root); err != nil {
		return err
	}
	return c.recordBuilt(id)
}

func (c *Controller) ReleaseUse(ctx context.Context, id string) error {
	previousPin := c.releasePin
	c.releasePin = id
	defer func() { c.releasePin = previousPin }()
	if _, err := c.release(id); err != nil {
		return err
	}
	if err := c.Down(); err != nil {
		return err
	}
	if err := c.ensureProviderKeys(ctx); err != nil {
		return err
	}
	if err := c.space(); err != nil {
		return err
	}
	snapshot, err := c.snapshots().
		Create(checkpoint.Manifest{Release: c.State.Release, Reason: "activation", Versions: c.versions()})
	if err != nil {
		return err
	}
	intent := activation{From: c.State.Release, To: id, Checkpoint: snapshot.ID}
	if err = platform.WriteJSON(filepath.Join(c.Root, "activation.json"), intent); err != nil {
		return err
	}
	c.State.Previous = c.State.Release
	c.State.Release = id
	c.State.Activation = snapshot.ID
	c.State.Mode = "stable"
	c.State.Session = checkpoint.ID()
	if err = c.infrastructure(ctx); err == nil {
		err = c.migrateRelease(ctx, id)
	}
	if err != nil {
		return errors.Join(err, c.recoverActivation())
	}
	if err = c.stopAll(); err != nil {
		return err
	}
	if err = c.save(); err != nil {
		return err
	}
	intent.Phase = "committed"
	if err = platform.WriteJSON(filepath.Join(c.Root, "activation.json"), intent); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(c.Root, "activation.json")); err != nil {
		_ = c.stopAll()
		return err
	}
	if err = platform.SyncDir(c.Root); err != nil {
		return err
	}
	return c.Up(ctx)
}

type activation struct{ From, To, Checkpoint, Phase string }

func (c *Controller) recoverActivation() error {
	var intent activation
	path := filepath.Join(c.Root, "activation.json")
	if err := platform.ReadJSON(path, &intent); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if intent.Phase == "committed" {
		if err := os.Remove(path); err != nil {
			return err
		}
		return platform.SyncDir(c.Root)
	}
	if err := c.stopAll(); err != nil {
		return err
	}
	if err := c.snapshots().Restore(intent.Checkpoint); err != nil {
		return err
	}
	c.State.Release = intent.From
	c.State.Activation = intent.Checkpoint
	c.State.Mode = "stopped"
	if err := c.save(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return platform.SyncDir(c.Root)
}

func (c *Controller) ReleaseRollback() error {
	if c.State.Activation == "" {
		return errors.New("no previous activation checkpoint")
	}
	return c.SnapshotRestore(c.State.Activation)
}

func (c *Controller) migrateRelease(ctx context.Context, id string) error {
	return c.migrateBinary(ctx, filepath.Join(c.Root, "releases", id, "tree-eclass"))
}

func (c *Controller) migrateBinary(ctx context.Context, binary string) error {
	path := filepath.Join(c.Root, "run", "release-migration.json")
	if err := platform.WriteJSON(path, map[string]string{"database_url": c.databaseURL()}); err != nil {
		return err
	}
	defer os.Remove(path)
	return c.runMigration(ctx, binary, []string{"TREE_RUNTIME_CONFIG=" + path})
}
