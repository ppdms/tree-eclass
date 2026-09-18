package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// This development-only process compiles assets to disk. It opens no HTTP port;
// Go serves both the browser application and its API in every runtime mode.
func (c *Controller) startFrontendBuild(ctx context.Context, root string) error {
	if c.State.Mode != "development" {
		return errors.New("frontend compiler is development-only")
	}
	if c.Processes.Alive("frontend-build") {
		return nil
	}
	out := filepath.Join(c.Root, "build/frontend")
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	args := []string{c.Config.Bun, "run", "--cwd", filepath.Join(root, "frontend"), "dev"}
	if err := c.start(ctx, "frontend-build", args, []string{"TREE_FRONTEND_OUT=" + out}, 0); err != nil {
		return err
	}
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if !c.Processes.Alive("frontend-build") {
			return errors.New("frontend compiler stopped; see ./tree logs frontend-build")
		}
		if _, err := os.Stat(filepath.Join(out, "index.html")); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("frontend build did not finish; see ./tree logs frontend-build")
		case <-tick.C:
		}
	}
}
