package workflow

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/storage"
)

func (c *Controller) infrastructure(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if err := c.freePorts(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(c.Root, "run"), 0700); err != nil {
		return err
	}
	store, err := blob.New(c.objectsRoot())
	if err != nil {
		return err
	}
	return store.Setup(ctx)
}

func (c *Controller) start(ctx context.Context, name string, argv, env []string, signal int) error {
	childCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return c.Processes.Start(
		childCtx,
		process.Spec{Name: name, Token: rand.Text(), Command: argv, Dir: c.Repo, Env: env, StopSignal: signal},
	)
}

func (c *Controller) stopAll() error {
	if err := c.Processes.Stop("watcher"); err != nil {
		return err
	}
	return c.stopDataset()
}

func (c *Controller) stopDataset() error {
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection"} {
		if err := c.Processes.Stop(name); err != nil {
			return err
		}
		if name == "api" {
			if err := process.StopHelpers(filepath.Join(c.Root, "run/jobs/.helpers")); err != nil {
				return err
			}
		}
	}
	return c.datasetStopped()
}

func (c *Controller) stopped() error {
	if c.Processes.Alive("watcher") {
		return errors.New("development watcher still owns its runtime lock")
	}
	return c.datasetStopped()
}

func (c *Controller) datasetStopped() error {
	if err := process.HelpersStopped(filepath.Join(c.Root, "run/jobs/.helpers")); err != nil {
		return err
	}
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection"} {
		if c.Processes.Alive(name) {
			return fmt.Errorf("%s still owns its runtime lock", name)
		}
	}
	if err := c.freePorts(); err != nil {
		return err
	}
	// This also catches children surviving a supervisor crash before PID publication.
	cmd := exec.Command("lsof", "-t", "+D", c.active())
	out, err := cmd.Output()
	if len(strings.TrimSpace(string(out))) > 0 {
		return errors.New("open files remain in the active dataset; refusing checkpoint")
	}
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			return fmt.Errorf("cannot verify closed dataset: %w", err)
		}
	}
	return nil
}

func (c *Controller) freePorts() error {
	port := c.Config.Ports.HTTP
	if port == 0 {
		return nil
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return fmt.Errorf("port %d is occupied; refusing to replace an unmanaged service", port)
	}
	listener.Close()
	return nil
}

func (c *Controller) migrate(ctx context.Context) error {
	return storage.MigrateConfig(ctx, c.storageConfig())
}

func (c *Controller) runMigration(ctx context.Context, binary string, env []string) error {
	return c.Processes.Run(
		ctx,
		process.Spec{
			Name:    "migration",
			Token:   rand.Text(),
			Command: []string{binary, "migrate"},
			Dir:     c.Repo,
			Env:     env,
		},
	)
}

func (c *Controller) stopAPI() error {
	if err := c.Processes.Stop("api"); err != nil {
		return err
	}
	return process.StopHelpers(filepath.Join(c.Root, "run/jobs/.helpers"))
}
