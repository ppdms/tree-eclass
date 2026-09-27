package workflow

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"tree-eclass/internal/domain/platform"
)

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
	cfg.Ports = Ports{Postgres: 15432, HTTP: 8000}
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
