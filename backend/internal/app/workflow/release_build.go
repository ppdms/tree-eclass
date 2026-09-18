package workflow

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func archiveSource(ctx context.Context, repo, revision, target string) error {
	cmd := exec.CommandContext(ctx, "git", "archive", "--format=tar", revision)
	cmd.Dir = repo
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	archive := tar.NewReader(pipe)
	readErr := extractSource(archive, target)
	if readErr != nil {
		_ = cmd.Process.Kill()
	}
	return errors.Join(readErr, cmd.Wait())
}
func extractSource(archive *tar.Reader, target string) error {
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if !filepath.IsLocal(header.Name) {
			return errors.New("unsafe archive path")
		}
		path := filepath.Join(target, header.Name)
		switch header.Typeflag {
		case tar.TypeXGlobalHeader:
			// git archive emits the commit ID as global PAX metadata, not a file.
			continue
		case tar.TypeDir:
			if err = os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(header.Mode)&0777)
			if err != nil {
				return err
			}
			_, err = io.Copy(f, archive)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("unsupported release source entry %s", header.Name)
		}
	}
}

func (c *Controller) buildArtifacts(ctx context.Context, root, id string) (Release, error) {
	release := Release{ID: id, Commit: id, Built: time.Now().UTC()}
	if err := c.run(ctx, root, "go", "-C", "backend", "test", "./cmd/...", "./internal/..."); err != nil {
		return release, err
	}
	if err := c.run(ctx, root, "go", "-C", "backend", "build", "-trimpath", "-ldflags=-s -w", "-o", filepath.Join(root, "tree-eclass"), "./cmd/tree-eclass"); err != nil {
		return release, err
	}
	frontend := filepath.Join(root, "frontend")
	if err := c.run(ctx, frontend, c.Config.Bun, "install", "--frozen-lockfile"); err != nil {
		return release, err
	}
	cmd := exec.CommandContext(ctx, c.Config.Bun, "run", "build")
	cmd.Dir = frontend
	cmd.Env = c.buildEnvironment()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return release, err
	}
	if err := packageFrontend(frontend); err != nil {
		return release, err
	}
	if err := c.packagePython(ctx, root); err != nil {
		return release, err
	}
	if err := packageParserSource(root); err != nil {
		return release, err
	}
	if err := c.packageNativeHelpers(ctx, root); err != nil {
		return release, err
	}

	// Read the release's migration manifest from its own code, never the controller revision.
	manifest, err := exec.CommandContext(ctx, filepath.Join(root, "tree-eclass"), "manifest").Output()
	if err != nil {
		return release, err
	}
	if err = json.Unmarshal(manifest, &release.Migrations); err != nil {
		return release, err
	}
	release.Files, err = releaseFiles(root)
	return release, err
}

func packageFrontend(frontend string) error {
	output := filepath.Join(frontend, "dist")
	if _, err := os.Stat(filepath.Join(output, "index.html")); err != nil {
		return err
	}
	packaged := frontend + "-packaged"
	if err := os.Rename(output, packaged); err != nil {
		return err
	}
	if err := os.RemoveAll(frontend); err != nil {
		return err
	}
	return os.Rename(packaged, frontend)
}
