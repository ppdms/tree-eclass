package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/storage"
)

// The fixture runs the real Go API with external work disabled. Only the small
// frontend transport and failure injection are doubles; no provider is contacted.
func fixtureApplication() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) > 1 {
		if os.Args[1] == "migrate" {
			executable, _ := os.Executable()
			if _, err := os.Stat(filepath.Join(filepath.Dir(executable), "fail-migration")); err == nil {
				cfg, err := server.Load(os.Getenv("TREE_RUNTIME_CONFIG"))
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 1
				}
				conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
				if err != nil {
					return 1
				}
				defer conn.Close(ctx)
				_, err = conn.Exec(ctx, `CREATE TABLE app.partially_migrated(id bigint)`)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
				fmt.Fprintln(os.Stderr, "synthetic migration failure")
				return 1
			}
		}
		if err := server.Run(ctx, os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	cfg, err := server.Load(os.Getenv("TREE_RUNTIME_CONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	cfg.ExternalWorkers = false
	if application := os.Getenv("TREE_FIXTURE_APPLICATION"); application != "" {
		if err = platform.WriteJSON(os.Getenv("TREE_RUNTIME_CONFIG"), cfg); err == nil {
			err = syscall.Exec(application, []string{application}, os.Environ())
		}
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err = server.Serve(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
func fixtureRelease(t *testing.T, c *Controller, id string, fail bool) {
	t.Helper()
	root := filepath.Join(c.Root, "releases", id)
	if err := os.MkdirAll(filepath.Join(root, "frontend"), 0700); err != nil {
		t.Fatal(err)
	}
	// Hard links keep these immutable test binaries from multiplying disk use.
	if err := os.Link(c.Executable, filepath.Join(root, "tree-eclass")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "frontend/index.html"), []byte(fixtureHTML), 0600); err != nil {
		t.Fatal(err)
	}
	if fail {
		if err := os.WriteFile(filepath.Join(root, "fail-migration"), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	fixtureRuntime(t, c, root)
	files, err := releaseFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := storage.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if err = platform.WriteJSON(
		filepath.Join(root, "release.json"),
		Release{
			ID:         id,
			Commit:     id,
			Built:      time.Now(),
			Files:      files,
			Migrations: manifest,
		},
	); err != nil {
		t.Fatal(err)
	}
}
func stoppedFixture(t *testing.T, c *Controller) {
	t.Helper()
	if err := c.stopped(); err != nil {
		t.Fatal("runtime not completely stopped", err)
	}
	for _, service := range []string{"api", "frontend-build", "frontend", "postgres", "seaweed"} {
		if c.Processes.Alive(service) {
			t.Fatal("writer survived shutdown", service)
		}
	}
	if c.State.Mode != "stopped" || c.State.Baseline != "" {
		t.Fatal("uncleared lifecycle state", c.State)
	}
}
func requireFixtureConfig(t *testing.T, c *Controller) {
	t.Helper()
	c.Config.Tessdata = os.Getenv("TREE_TEST_TESSDATA")
	if c.Config.Tessdata == "" {
		t.Skip("set TREE_TEST_TESSDATA for complete native lifecycle checks")
	}
	var err error
	c.Config.Bun, err = exec.LookPath("bun")
	if err != nil {
		t.Fatal(err)
	}
	c.Repo = t.TempDir()
	if err = os.MkdirAll(filepath.Join(c.Repo, "frontend"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = platform.WriteJSON(c.providerPath(), providerSettings{Format: 1, Keys: map[string]string{}}); err != nil {
		t.Fatal(err)
	}
	if err = c.save(); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func fixtureRuntime(t *testing.T, c *Controller, root string) {
	t.Helper()
	path := filepath.Join(root, "runtime")
	if err := os.MkdirAll(filepath.Join(path, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	if c.Config.Tessdata != "" {
		if err := cloneArtifact(c.Config.Tessdata, filepath.Join(path, "tessdata")); err != nil {
			t.Fatal(err)
		}
	}
	// Helpers are exercised separately by the complete packaged-parser fixture.
	if err := platform.WriteJSON(filepath.Join(path, "runtime.json"), runtimeTools{1, strings.Repeat("0", 64)}); err != nil {
		t.Fatal(err)
	}
}

const fixtureHTML = `<html data-tree-runtime="__TREE_RUNTIME__" data-tree-storage="__TREE_STORAGE__" data-tree-mode="__TREE_MODE__" data-tree-build="__TREE_BUILD__"><body>Browser fixture</body></html>`
