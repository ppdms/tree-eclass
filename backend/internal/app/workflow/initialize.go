package workflow

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"tree-eclass/internal/domain/platform"
)

func (c *Controller) run(ctx context.Context, dir string, argv ...string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = c.buildEnvironment()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (c *Controller) initialize(ctx context.Context) error {
	temp, err := os.MkdirTemp(c.Root, ".setup-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	password := filepath.Join(temp, "password")
	if err = os.WriteFile(password, []byte(c.Config.Password), 0600); err != nil {
		return err
	}
	err = c.run(
		ctx,
		c.Repo,
		filepath.Join(c.Config.PostgresBin, "initdb"),
		"-D",
		filepath.Join(temp, "postgres"),
		"-U",
		"tree",
		"--pwfile="+password,
		"--auth-host=scram-sha-256",
		"--auth-local=trust",
		"--encoding=UTF8",
		"--locale=C.UTF-8",
	)
	if err != nil {
		return err
	}
	if err = os.Remove(password); err != nil {
		return err
	}
	for _, dir := range []string{"objects", "settings", "exports"} {
		if err = os.Mkdir(filepath.Join(temp, dir), 0700); err != nil {
			return err
		}
	}
	if err = platform.WriteJSON(
		filepath.Join(temp, "dataset.json"),
		map[string]any{
			"format":   2,
			"postgres": c.Config.PostgresVersion,
			"objects":  objectsFormat,
		},
	); err != nil {
		return err
	}
	if err = platform.SyncTree(temp); err != nil {
		return err
	}
	if err = os.Rename(temp, c.active()); err != nil {
		return err
	}
	return platform.SyncDir(c.Root)
}

func (c *Controller) localRemote(ctx context.Context) error {
	path := filepath.Join(filepath.Dir(c.Root), "source.git")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = c.run(ctx, c.Repo, "git", "init", "--bare", path); err != nil {
			return err
		}
	}
	cmd := exec.CommandContext(ctx, "git", "remote", "get-url", "laptop")
	cmd.Dir = c.Repo
	output, err := cmd.Output()
	if err == nil && strings.TrimSpace(string(output)) != path {
		return fmt.Errorf(
			"laptop remote already targets a different path; preserved %s",
			strings.TrimSpace(string(output)),
		)
	}
	if err != nil {
		if err = c.run(ctx, c.Repo, "git", "remote", "add", "laptop", path); err != nil {
			return err
		}
	}
	if err = c.run(ctx, c.Repo, "git", "push", "laptop", "--all"); err != nil {
		return err
	}
	if err = c.run(ctx, c.Repo, "git", "push", "laptop", "--tags"); err != nil {
		return err
	}
	return c.run(ctx, c.Repo, "git", "config", "remote.pushDefault", "laptop")
}
