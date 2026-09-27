package workflow

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"tree-eclass/internal/domain/platform"
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
	runDir := filepath.Join(c.Root, "run")
	if err := os.MkdirAll(runDir, 0700); err != nil {
		return err
	}
	pgArgs := []string{filepath.Join(c.Config.PostgresBin, "postgres"), "-D", filepath.Join(c.active(), "postgres"),
		"-h", "127.0.0.1", "-p", fmt.Sprint(c.Config.Ports.Postgres), "-k", "",
		"-c", "shared_buffers=32MB", "-c", "work_mem=2MB", "-c", "maintenance_work_mem=32MB", "-c", "max_connections=16", "-c", "max_parallel_workers=0"}
	if err := c.start(ctx, "postgres", pgArgs, nil, int(syscall.SIGINT)); err != nil {
		return err
	}
	if err := c.waitDatabase(ctx); err != nil {
		return err
	}
	if err := c.startWeed(ctx, runDir); err != nil {
		return err
	}
	return nil
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

func (c *Controller) startWeed(ctx context.Context, runDir string) error {
	config := filepath.Join(c.Root, "s3.json")
	identity := map[string]any{
		"identities": []any{
			map[string]any{
				"name":        "tree",
				"credentials": []any{map[string]string{"accessKey": c.Config.S3Access, "secretKey": c.Config.S3Secret}},
				"actions":     []string{"Admin", "Read", "Write", "List", "Tagging"},
			},
		},
	}
	if err := platform.WriteJSON(config, identity); err != nil {
		return err
	}
	p := c.Config.Ports
	args := []string{
		c.Config.Weed,
		"mini",
		"-dir=" + filepath.Join(c.active(), "seaweed"),
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-master.port=" + fmt.Sprint(
			p.Master,
		),
		"-volume.port=" + fmt.Sprint(p.Volume),
		"-filer.port=" + fmt.Sprint(p.Filer),
		"-s3.port=" + fmt.Sprint(p.S3),
		"-admin.port=" + fmt.Sprint(p.Admin),
		"-admin.dataDir=" + filepath.Join(c.active(), "seaweed", "admin"),
		"-master.telemetry=false",
		"-admin.ui=false",
		"-webdav=false",
		"-s3.port.iceberg=0",
		"-s3.port.lance=0",
		"-s3.iam=false",
		"-s3.config=" + config,
		"-s3.autoCreateBucket=false",
		"-s3.allowDeleteBucketNotEmpty=false",
		"-s3.cacheCapacityMB=0",
		"-filer.localSocket=" + c.socketPath("filer"),
		"-s3.localSocket=" + c.socketPath("s3"),
		"-master.volumeSizeLimitMB=128",
		"-volume.max=64",
		"-volume.index=leveldb",
		"-volume.readBufferSizeMB=1",
		"-filer.concurrentFileUploadLimit=1",
		"-s3.concurrentFileUploadLimit=1",
		"-s3.concurrentUploadLimitMB=64",
		"-volume.concurrentUploadLimitMB=64",
		"-volume.concurrentDownloadLimitMB=64",
		"-filer.disableDirListing=true",
		"-filer.exposeDirectoryData=false",
	}
	if err := c.start(ctx, "seaweed", args, []string{"GOMEMLIMIT=192MiB"}, int(syscall.SIGTERM)); err != nil {
		return err
	}
	store, err := blob.New(c.endpoint(), c.Config.S3Access, c.Config.S3Secret)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	for {
		err = store.Setup(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("SeaweedFS readiness: %w", err)
		}
		retry.Reset(200 * time.Millisecond)
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
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection", "seaweed", "postgres"} {
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
	for _, name := range []string{"frontend-build", "frontend", "api", "migration", "collection", "seaweed", "postgres"} {
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
	for _, port := range []int{
		p.HTTP,
		p.Postgres,
		p.S3,
		p.Master,
		p.Volume,
		p.Filer,
		p.Admin,
		p.Master + 10000,
		p.Volume + 10000,
		p.Filer + 10000,
		p.S3 + 10000,
		p.Admin + 10000,
	} {
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

func (c *Controller) migrate(ctx context.Context) error { return storage.Migrate(ctx, c.databaseURL()) }

func (c *Controller) socketPath(name string) string {
	hash := sha256.Sum256([]byte(c.Root))
	return fmt.Sprintf("/tmp/tree-eclass-%d-%x-%s.sock", os.Getuid(), hash[:8], name)
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
