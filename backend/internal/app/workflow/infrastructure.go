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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/storage"
)

func (c *Controller) infrastructure(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if err := c.verifyStorageTools(ctx); err != nil {
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
	if err = store.Setup(ctx); err != nil {
		return err
	}
	if c.sqliteSelected() {
		return nil
	}
	if err := c.startPostgres(ctx); err != nil {
		return err
	}
	return c.waitDatabase(ctx)
}

func (c *Controller) startPostgres(ctx context.Context) error {
	return c.start(ctx, "postgres", []string{
		filepath.Join(c.Config.PostgresBin, "postgres"), "-D", filepath.Join(c.active(), "postgres"),
		"-h", "127.0.0.1", "-p", fmt.Sprint(c.Config.Ports.Postgres), "-k", "",
		"-c", "shared_buffers=32MB", "-c", "work_mem=2MB", "-c", "maintenance_work_mem=32MB",
		"-c", "max_connections=16", "-c", "max_parallel_workers=0",
	}, nil, int(syscall.SIGINT))
}

func (c *Controller) start(ctx context.Context, name string, argv, env []string, signal int) error {
	childCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return c.Processes.Start(
		childCtx,
		process.Spec{Name: name, Token: rand.Text(), Command: argv, Dir: c.Repo, Env: env, StopSignal: signal},
	)
}

func (c *Controller) waitDatabase(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(c.databaseURL())
	if err != nil {
		return err
	}
	cfg.Database = "postgres"
	deadline := time.Now().Add(20 * time.Second)
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	for {
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err == nil {
			defer conn.Close(ctx)
			var exists bool
			if err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname='tree')").Scan(&exists); err != nil {
				return err
			}
			if !exists {
				_, err = conn.Exec(ctx, "CREATE DATABASE tree")
			}
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("native PostgreSQL did not become ready; see ./tree logs postgres")
		}
		retry.Reset(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry.C:
		}
	}
}

func (c *Controller) stopAll() error {
	if err := c.Processes.Stop("watcher"); err != nil {
		return err
	}
	return c.stopDataset()
}

func (c *Controller) stopDataset() error {
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection", "postgres"} {
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
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection", "postgres"} {
		if c.Processes.Alive(name) {
			return fmt.Errorf("%s still owns its runtime lock", name)
		}
	}
	if err := c.freePorts(); err != nil {
		return err
	}
	pg := filepath.Join(c.active(), "postgres")
	if _, err := os.Stat(pg); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	cmd := exec.Command(filepath.Join(c.Config.PostgresBin, "pg_controldata"), pg)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	clean := false
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "Database cluster state:") &&
			strings.TrimSpace(strings.TrimPrefix(line, "Database cluster state:")) == "shut down" {
			clean = true
		}
	}
	if !clean {
		return errors.New("PostgreSQL has not shut down cleanly; checkpoint refused")
	}
	// This also catches children surviving a supervisor crash before PID publication.
	cmd = exec.Command("lsof", "-t", "+D", c.active())
	out, err = cmd.Output()
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
	p := c.Config.Ports
	for _, port := range []int{p.HTTP, p.Postgres} {
		if port == 0 {
			continue
		}
		listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return fmt.Errorf("port %d is occupied; refusing to replace an unmanaged service", port)
		}
		listener.Close()
	}
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
