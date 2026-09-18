package workflow

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

const pythonVersion = "3.14.7"
const pythonBuild = "20260901"
const pythonArchiveSHA = "4632cb1a6edad9e73d3c81b6d2e69131637d995173e3e85005df14102b0592ba"

const pythonArchiveURL = "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.14.7%2B20260901-aarch64-apple-darwin-install_only_stripped.tar.gz"

type pythonDistribution struct {
	Version, Build, ArchiveSHA string
	Files                      map[string]string
}

func (c *Controller) pythonBase() string {
	return filepath.Join(c.Root, "tools", "python-"+pythonVersion+"-"+pythonBuild)
}
func verifyPythonBase(root string) error {
	var distribution pythonDistribution
	if err := platform.ReadJSON(filepath.Join(root, "distribution.json"), &distribution); err != nil {
		return err
	}
	if distribution.Version != pythonVersion || distribution.Build != pythonBuild ||
		distribution.ArchiveSHA != pythonArchiveSHA {
		return errors.New("Python distribution identity changed")
	}
	if err := ensurePythonExecutable(filepath.Join(root, "python")); err != nil {
		return err
	}
	return verifyRelease(filepath.Join(root, "python"), Release{Files: distribution.Files})
}
func installPythonBase(ctx context.Context, root string) error {
	if _, err := os.Lstat(root); err == nil {
		return verifyPythonBase(root)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		return errors.New("native Python provisioning requires macOS ARM64")
	}
	if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		return err
	}
	temp, err := os.MkdirTemp(filepath.Dir(root), ".python-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	archive, err := verifiedDownload(ctx, temp, pythonArchiveURL, pythonArchiveSHA, 64<<20)
	if err != nil {
		return err
	}
	defer archive.Close()
	gz, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer gz.Close()
	if err = extractPython(tar.NewReader(gz), temp); err != nil {
		return err
	}
	archive.Close()
	if err = os.Remove(archive.Name()); err != nil {
		return err
	}
	files, err := releaseFiles(filepath.Join(temp, "python"))
	if err != nil {
		return err
	}
	distribution := pythonDistribution{pythonVersion, pythonBuild, pythonArchiveSHA, files}
	if err = platform.WriteJSON(filepath.Join(temp, "distribution.json"), distribution); err != nil {
		return err
	}
	if err = platform.SyncTree(temp); err != nil {
		return err
	}
	if err = os.Rename(temp, root); err != nil {
		return err
	}
	return platform.SyncDir(filepath.Dir(root))
}

func verifiedDownload(ctx context.Context, dir, url, expected string, limit int64) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dependency download HTTP %d", response.StatusCode)
	}
	file, err := os.CreateTemp(dir, "download-*")
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit || fmt.Sprintf("%x", hash.Sum(nil)) != expected {
		return nil, errors.New("dependency archive checksum or size mismatch")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	ok = true
	return file, nil
}
