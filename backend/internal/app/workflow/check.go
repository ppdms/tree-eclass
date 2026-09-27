package workflow

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"tree-eclass/internal/infrastructure/checkpoint"
)

func (c *Controller) Check(ctx context.Context) (err error) {
	if err = c.configured(); err != nil {
		return err
	}
	parserBase, err := c.preparedParser()
	if err != nil {
		return err
	}
	resume := c.Processes.Alive("api") || c.Processes.Alive("frontend-build") || c.Processes.Alive("frontend")
	if err = c.stopAll(); err != nil {
		return err
	}
	if c.State.Mode == "development" && c.State.Baseline != "" {
		if err = c.space(); err != nil {
			return err
		}
		if _, err = c.snapshots().Create(checkpoint.Manifest{
			Release:     c.State.Release,
			Reason:      "development-check",
			Development: c.State.Baseline,
			Versions:    c.versions(),
		}); err != nil {
			return err
		}
	}
	// A check temporarily suspends a development session; it does not discard it.
	defer func() {
		err = errors.Join(err, c.cleanBuild())
		if resume {
			restore, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			err = errors.Join(err, c.launch(restore))
		}
	}()
	application := filepath.Join(c.Root, "build/validation/tree-eclass")
	if err = os.MkdirAll(filepath.Dir(application), 0700); err != nil {
		return err
	}
	checks := [][]string{
		{c.Config.Bun, "run", "--cwd", filepath.Join(c.Repo, "frontend"), "build"},
		{"go", "-C", "backend", "build", "-trimpath", "-ldflags=-s -w", "-o", application, "./cmd/tree-eclass"},
		{"go", "-C", "backend", "test", "-race", "-count=1", "-parallel=4", "./cmd/...", "./internal/..."},
		{"go", "-C", "backend", "vet", "./cmd/...", "./internal/..."},
		{"python3", "scripts/quality/audit_limits.py"},
		{"ruff", "check", "."},
		{"ruff", "format", "--check", "."},
		{filepath.Join(c.Repo, "frontend/node_modules/.bin/oxfmt"), "--check", "."},
		{c.Config.Bun, "run", "--cwd", filepath.Join(c.Repo, "frontend"), "lint"},
		{c.Config.Bun, "test", "frontend/tools/tests"},
	}
	for _, args := range checks {
		fmt.Printf("Checking %s\n", args[0])
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = c.Repo
		cmd.Env = append(
			c.buildEnvironment(),
			"TREE_NATIVE_TESTS=1",
			"TREE_TEST_TESSDATA="+c.Config.Tessdata,
			"TREE_TEST_DISCORD="+c.Config.DiscordExporter,
			"TREE_TEST_PYTHON_BASE="+c.pythonBase(),
			"TREE_TEST_PARSER_BASE="+parserBase,
			"TREE_TEST_APPLICATION="+application,
			"TREE_NATIVE_TOOLS_TEST=1",
			"TREE_FRONTEND_OUT="+filepath.Join(c.Repo, "frontend/dist/validation"),
			"TREE_TEST_FRONTEND_BUILD="+filepath.Join(c.Repo, "frontend/dist/validation"),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err = cmd.Run(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) Clean() error {
	if c.Processes.Alive("api") || c.Processes.Alive("frontend-build") || c.Processes.Alive("frontend") ||
		c.Processes.Alive("watcher") ||
		c.Processes.Alive("migration") ||
		c.Processes.Alive("collection") {
		return errors.New("stop the application and its helpers before cleaning build output")
	}
	if err := c.cleanBuild(); err != nil {
		return err
	}
	return c.prune()
}
func (c *Controller) cleanBuild() error {
	for _, path := range []string{
		filepath.Join(c.Root, "cache"),
		filepath.Join(c.Root, "build"),
		filepath.Join(c.Repo, "frontend/dist/validation"),
		filepath.Join(c.Root, "run/jobs"),
		filepath.Join(c.Root, "run/frontend"),
	} {
		if err := writableDirectories(path); err != nil {
			return err
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}

// Go's downloaded module directories are read-only. Only traverse the owned
// cleanup roots, and never follow symlinks into other projects or stored data.
func writableDirectories(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0700)
		}
		return nil
	})
}

func (c *Controller) buildEnvironment() []string {
	root := c.buildCache
	if root == "" {
		root = filepath.Join(c.Root, "cache")
	}
	env := make([]string, 0, len(os.Environ())+4)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if name == "TREE_NATIVE_TESTS" || name == "TREE_NATIVE_TOOLS_TEST" ||
			name == "TREE_RUNTIME_CONFIG" || name == "TREE_FIXTURE_APPLICATION" ||
			strings.HasPrefix(name, "TREE_TEST_") {
			continue
		}
		env = append(env, value)
	}
	return append(env,
		"GOCACHE="+filepath.Join(root, "go-build"),
		"GOMODCACHE="+filepath.Join(root, "go-mod"),
		"UV_CACHE_DIR="+filepath.Join(root, "uv"),
		"BUN_INSTALL_CACHE_DIR="+filepath.Join(root, "bun"))
}

// Release builds can run while the development app is alive. Their cleanup must
// never remove its binary, parser workspace, browser build output or dependency cache.
func (c *Controller) releaseBuildCache() func() error {
	previous := c.buildCache
	root := filepath.Join(c.Root, "cache", "release")
	c.buildCache = root
	return func() error {
		c.buildCache = previous
		if err := writableDirectories(root); err != nil {
			return err
		}
		return os.RemoveAll(root)
	}
}
