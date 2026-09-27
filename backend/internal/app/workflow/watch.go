package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/platform"
)

// Watch is a managed child, not a login service. Compilation does not hold the
// lifecycle lock; shutdown drains its whole group before cleaning its workspace.
func Watch(ctx context.Context, root, repo, session string) error {
	c := &Controller{Root: root, Repo: repo}
	if err := platform.ReadJSON(filepath.Join(root, "config.json"), &c.Config); err != nil {
		return err
	}
	var active server.Config
	if err := platform.ReadJSON(filepath.Join(root, "run/application.json"), &active); err != nil {
		return err
	}
	last, observed := active.Code, ""
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		digest, err := sourceDigest(repo, "")
		if err != nil {
			fmt.Println("Source scan:", err)
			continue
		}
		if digest == last {
			continue
		}
		if digest != observed {
			observed = digest
			continue
		} // Debounce multi-file saves.
		last = digest
		if err = c.rebuild(ctx, session); err != nil {
			fmt.Println("Go reload:", err)
			if errors.Is(err, errSourceChanged) {
				last = ""
			}
		}
	}
}

func (c *Controller) rebuild(ctx context.Context, session string) error {
	root := filepath.Join(c.Root, "build")
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	candidate, err := os.MkdirTemp(root, "reload-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(candidate)
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	binary := filepath.Join(candidate, "tree-eclass")
	fmt.Println("Compiling Go changes…")
	digest, err := c.buildDevelopment(ctx, binary)
	if err != nil {
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		owner, err := openAt(c.Repo, c.Root)
		if errors.Is(err, unix.EWOULDBLOCK) {
			retry.Reset(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-retry.C:
				continue
			}
		}
		if err != nil {
			return err
		}
		defer owner.Close()
		if owner.State.Mode != "development" || owner.State.Session != session || owner.State.Baseline == "" {
			return errors.New("development session ended before reload")
		}
		current, err := sourceDigest(c.Repo, "")
		if err != nil {
			return err
		}
		if current != digest {
			return errSourceChanged
		}
		if err = owner.replaceDevelopment(ctx, binary, digest); err != nil {
			return err
		}
		fmt.Println("Go API reloaded; development data and browser session preserved.")
		return nil
	}
}
