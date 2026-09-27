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

func (c *Controller) Doctor(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	if err := c.verifyStorageTools(ctx); err != nil {
		return err
	}
	if err := verifyTessdata(c.Config.Tessdata); err != nil {
		return err
	}
	if err := verifyDiscord(c.Config.DiscordExporter); err != nil {
		return err
	}
	free, err := platform.Available(c.Root)
	if err != nil {
		return err
	}
	fmt.Printf(
		"Native data: %s\nPostgreSQL: %s\nFree disk: %.2f GiB\n",
		c.active(),
		c.Config.PostgresVersion,
		float64(free)/(1024*1024*1024),
	)
	return c.Status()
}

func (c *Controller) verifyStorageTools(ctx context.Context) error {
	result, err := exec.CommandContext(ctx, filepath.Join(c.Config.PostgresBin, "postgres"), "--version").Output()
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(result)) != c.Config.PostgresVersion {
		return fmt.Errorf("PostgreSQL version changed; cold checkpoints require %s", c.Config.PostgresVersion)
	}
	return nil
}
func (c *Controller) Status() error {
	fmt.Printf("Mode: %s\nRelease: %s\n", c.State.Mode, c.State.Release)
	if c.State.Baseline != "" {
		fmt.Printf("Development baseline: %s (development writes will be discarded)\n", c.State.Baseline)
	}
	for _, name := range []string{"postgres", "api", "frontend-build", "frontend", "migration", "collection", "watcher"} {
		fmt.Printf("%s: %t\n", name, c.Processes.Alive(name))
	}
	return nil
}
func (c *Controller) Logs(name string) error {
	switch name {
	case "postgres", "api", "frontend-build", "frontend", "migration", "collection", "watcher":
	default:
		return fmt.Errorf("unknown service %q", name)
	}
	path := filepath.Join(c.Processes.Root, name, "output.log")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	offset := max(int64(0), info.Size()-64*1024)
	if _, err = f.Seek(offset, 0); err != nil {
		return err
	}
	b := make([]byte, 64*1024)
	n, err := f.Read(b)
	if n > 0 {
		_, _ = os.Stdout.Write(b[:n])
		return nil
	}
	return err
}
