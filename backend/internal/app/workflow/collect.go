package workflow

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/process"
)

// Collect runs the selected release's catalog contract, never the controller's
// possibly newer schema. It cannot silently stop or discard a development session.
func (c *Controller) Collect(ctx context.Context) (err error) {
	if err = c.configured(); err != nil {
		return err
	}
	if c.State.Mode != "stopped" || c.State.Baseline != "" {
		return errors.New("stop the application before collecting abandoned objects")
	}
	if _, err = os.Stat(filepath.Join(c.Root, "activation.json")); !errors.Is(err, os.ErrNotExist) {
		return errors.New("finish activation recovery before collecting objects")
	}
	if _, err = c.release(c.State.Release); err != nil {
		return err
	}
	if err = c.stopped(); err != nil {
		return err
	}
	if err = c.space(); err != nil {
		return err
	}
	if _, err = c.snapshots().Create(checkpoint.Manifest{
		Release:  c.State.Release,
		Reason:   "before-collection",
		Versions: c.versions(),
	}); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.stopAll()) }()
	if err = c.infrastructure(ctx); err != nil {
		return err
	}
	path := filepath.Join(c.Root, "run", "collection.json")
	cfg := c.collectionConfig()
	if err = platform.WriteJSON(path, cfg); err != nil {
		return err
	}
	defer os.Remove(path)
	binary := filepath.Join(c.Root, "releases", c.State.Release, "tree-eclass")
	if err = c.Processes.Run(ctx, c.collectionSpec(binary, path)); err != nil {
		return err
	}
	if err = c.Logs("collection"); err != nil {
		return err
	}
	fmt.Println(
		"Object collection completed; abandoned objects were removed from the filesystem store.",
	)
	return c.prune()
}

func (c *Controller) collectionConfig() server.Config {
	return server.Config{
		DatabaseURL: c.databaseURL(),
		ObjectsRoot: c.objectsRoot(),
	}
}

func (c *Controller) collectionSpec(binary, path string) process.Spec {
	return process.Spec{
		Name: "collection", Token: rand.Text(), Command: []string{binary, "collect"},
		Dir: c.Repo, Env: []string{"TREE_RUNTIME_CONFIG=" + path, "GOMEMLIMIT=192MiB"},
	}
}
