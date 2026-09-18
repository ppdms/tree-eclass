package workflow

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"tree-eclass/internal/infrastructure/platform"
)

const weedVersion = "4.46"
const weedDarwinARM64 = "1d26b6cd43a6ed4f8c315bac15a19334270e5c84505fbf7cc4fdb1e251a71bc5"
const weedBinaryDarwinARM64 = "b701007cd085dc5c0e5f4932ab3e1b997644345f9ddaef246753c0c87dd79500"

func (c *Controller) Setup(ctx context.Context) error {
	if err := c.requireStoppedSetup(); err != nil {
		return err
	}
	if c.Config.Format == 1 {
		return c.setupExisting(ctx)
	}
	return c.initializeSetup(ctx)
}

// setupExisting refreshes the helper tooling of an already initialized dataset.
func (c *Controller) setupExisting(ctx context.Context) error {
	pg, version, err := postgresDependency(ctx)
	if err != nil {
		return err
	}
	c.Config.PostgresBin, c.Config.PostgresVersion = pg, version
	if err := c.setupParser(ctx); err != nil {
		return err
	}
	if err := c.ensureProviderKeys(ctx); err != nil {
		return err
	}
	if err := installTessdata(ctx, c.tessdataPath()); err != nil {
		return err
	}
	c.Config.Tessdata = c.tessdataPath()
	if err := installDiscord(ctx, c.discordPath()); err != nil {
		return err
	}
	c.Config.DiscordExporter = c.discordPath()
	diff, checksum, err := c.setupPDFTool()
	if err != nil {
		return err
	}
	c.Config.PDFDiff, c.Config.PDFDiffSHA = diff, checksum
	if err := c.setupDevelopment(ctx); err != nil {
		return err
	}
	if err := c.writeSetupConfig(); err != nil {
		return err
	}
	if err := c.Doctor(ctx); err != nil {
		return err
	}
	return c.localRemote(ctx)
}

// initializeSetup provisions native storage for the first time on this machine.
func (c *Controller) initializeSetup(ctx context.Context) error {
	if err := c.space(); err != nil {
		return err
	}
	if _, err := os.Lstat(c.active()); !errors.Is(err, os.ErrNotExist) {
		return errors.New("unregistered active storage exists; refusing initialization")
	}
	cfg, err := c.dependencies(ctx)
	if err != nil {
		return err
	}
	if err = c.setupParser(ctx); err != nil {
		return err
	}
	cfg.Password = rand.Text()
	cfg.SourceRoot = c.Repo
	cfg.S3Access = rand.Text()
	cfg.S3Secret = rand.Text()
	cfg.Ports = Ports{15432, 8333, 9333, 9340, 8888, 23646, 8000}
	cfg.Format = 1
	c.Config = cfg
	if err = c.setupDevelopment(ctx); err != nil {
		return err
	}
	cfg = c.Config
	if err = c.initialize(ctx); err != nil {
		return err
	}
	if err = platform.WriteJSON(filepath.Join(c.Root, "config.json"), cfg); err != nil {
		return err
	}
	c.State = Selection{Mode: "stopped"}
	if err = c.save(); err != nil {
		return err
	}
	if err = c.ensureProviderKeys(ctx); err != nil {
		return err
	}
	if err = c.localRemote(ctx); err != nil {
		return err
	}
	fmt.Println(
		"Native storage initialized. Existing cache, credentials, Git history and old configuration are preserved.",
	)
	return nil
}

func (c *Controller) writeSetupConfig() error {
	if err := platform.WriteJSON(filepath.Join(c.Root, "config.json"), c.Config); err != nil {
		return err
	}
	if c.Config.SourceRoot == "" {
		c.Config.SourceRoot = c.Repo
		return platform.WriteJSON(filepath.Join(c.Root, "config.json"), c.Config)
	}
	return nil
}

func (c *Controller) dependencies(ctx context.Context) (Config, error) {
	var cfg Config
	var err error
	cfg.PostgresBin, cfg.PostgresVersion, err = postgresDependency(ctx)
	if err != nil {
		return cfg, err
	}
	cfg.Bun, err = exec.LookPath("bun")
	if err != nil {
		return cfg, err
	}
	for _, name := range []string{"tesseract", "pdftoppm", "pdftotext", "diff-pdf", "7zz"} {
		if _, err = exec.LookPath(name); err != nil {
			return cfg, fmt.Errorf("missing native document tool %s", name)
		}
	}
	cfg.Weed = filepath.Join(c.Root, "tools", "seaweedfs-"+weedVersion, "weed")
	if err = installWeed(ctx, cfg.Weed); err != nil {
		return cfg, err
	}
	cfg.WeedVersion = weedVersion
	cfg.PDFDiff, cfg.PDFDiffSHA, err = c.setupPDFTool()
	if err != nil {
		return cfg, err
	}
	cfg.DiscordExporter = c.discordPath()
	if err = installDiscord(ctx, cfg.DiscordExporter); err != nil {
		return cfg, err
	}
	cfg.Tessdata = c.tessdataPath()
	if err = installTessdata(ctx, cfg.Tessdata); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func postgresDependency(ctx context.Context) (string, string, error) {
	pg := os.Getenv("TREE_POSTGRES_BIN")
	if pg == "" {
		pg = "/opt/homebrew/opt/postgresql@18/bin"
	}
	pg, err := filepath.Abs(pg)
	if err != nil {
		return "", "", fmt.Errorf("set TREE_POSTGRES_BIN to PostgreSQL 18: %w", err)
	}
	version, err := exec.CommandContext(ctx, filepath.Join(pg, "postgres"), "--version").Output()
	if err != nil {
		return "", "", err
	}
	if !strings.Contains(string(version), " 18.") {
		return "", "", errors.New("PostgreSQL 18 is required")
	}
	return pg, strings.TrimSpace(string(version)), nil
}

func installWeed(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err == nil {
		return verifyWeed(path)
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		return errors.New(
			"native automatic SeaweedFS provisioning currently requires macOS ARM64; use the pinned server image on Linux",
		)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://github.com/seaweedfs/seaweedfs/releases/download/"+weedVersion+"/darwin_arm64.tar.gz",
		nil,
	)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("SeaweedFS download HTTP %d", response.StatusCode)
	}
	archive, err := os.CreateTemp(filepath.Dir(path), "download-*")
	if err != nil {
		return err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	hash := sha256.New()
	if _, err = io.Copy(io.MultiWriter(archive, hash), io.LimitReader(response.Body, 100*1024*1024)); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != weedDarwinARM64 {
		return errors.New("SeaweedFS archive checksum mismatch")
	}
	if _, err = archive.Seek(0, 0); err != nil {
		return err
	}
	gz, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer gz.Close()
	if err = extractWeed(tar.NewReader(gz), path); err != nil {
		return err
	}
	return verifyWeed(path)
}

func verifyWeed(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != weedBinaryDarwinARM64 {
		return errors.New("SeaweedFS binary checksum mismatch; preserved the file and refused to run it")
	}
	return nil
}

func extractWeed(archive *tar.Reader, path string) error {
	for {
		header, err := archive.Next()
		if err != nil {
			return err
		}
		if header.Name != "weed" {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size > 300*1024*1024 {
			return errors.New("invalid SeaweedFS archive entry")
		}
		f, err := os.OpenFile(path+".partial", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		_, err = io.Copy(f, archive)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err = os.Rename(f.Name(), path); err != nil {
			return err
		}
		return platform.SyncDir(filepath.Dir(path))
	}
}
