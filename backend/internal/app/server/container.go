package server

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/storage"
)

// Containers use the same finite-writer journal and helper ownership protocol.
// The orchestrator owns PostgreSQL and must checkpoint it while stopped
// before explicitly invoking container-migrate for an application upgrade.
func containerRuntime(ctx context.Context, cfg Config, migrate bool) (err error) {
	if !filepath.IsAbs(cfg.Temp) || cfg.Temp == "/" || cfg.Session == "" || cfg.Mode != "stable" {
		return errors.New(
			"container runtime requires a private absolute temp directory, stable mode and a runtime session",
		)
	}
	if err = os.MkdirAll(cfg.Temp, 0700); err != nil {
		return err
	}
	lock, err := platform.Lock(filepath.Join(cfg.Temp, "container.lock"))
	if err != nil {
		return err
	}
	defer platform.Unlock(lock)
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	manager := process.Manager{Root: filepath.Join(cfg.Temp, ".processes"), Executable: executable}
	stop := func() error { return stopContainer(manager, cfg.Temp) }
	if err = stop(); err != nil {
		return err
	}
	// A stop that was requested (SIGTERM/SIGINT cancels ctx) is a clean shutdown:
	// report success so the unit does not look failed to systemd and
	// Restart=on-failure keeps restarting only genuine crashes.
	defer func() {
		stopErr := stop()
		if ctx.Err() == nil {
			err = errors.Join(err, stopErr)
		}
	}()
	configPath := filepath.Join(cfg.Temp, "container-runtime.json")
	if err = containerTools(&cfg); err != nil {
		return err
	}
	if err = platform.WriteJSON(configPath, cfg); err != nil {
		return err
	}
	defer os.Remove(configPath)
	spec := process.Spec{Name: "api", Token: rand.Text(), Command: []string{executable}, Dir: cfg.ParserRoot}
	spec.Env = []string{"TREE_RUNTIME_CONFIG=" + configPath}
	if migrate {
		spec.Name = "migration"
		spec.Command = append(spec.Command, "migrate")
	}
	if err = manager.Run(ctx, spec); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if !migrate {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("application stopped unexpectedly")
	}
	return setupContainerObjects(ctx, cfg)
}

func stopContainer(manager process.Manager, root string) error {
	if err := manager.Stop("api"); err != nil {
		return err
	}
	if err := manager.Stop("migration"); err != nil {
		return err
	}
	return process.StopHelpers(filepath.Join(root, ".helpers"))
}

func setupContainerObjects(ctx context.Context, cfg Config) (err error) {
	// Keep exclusive DB admission while configuring the application's object store.
	db, err := storage.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	objects, err := blob.New(cfg.ObjectsRoot)
	if err != nil {
		return err
	}
	return objects.Setup(ctx)
}

func containerTools(cfg *Config) error {
	// Helpers and resources are part of the image, never injected host binaries.
	cfg.ParserRoot = "/app"
	cfg.FrontendDir = "/app/frontend"
	cfg.ParserPython = "/usr/local/bin/python3.14"
	cfg.NativeToolsRoot = ""
	cfg.DiscordExporter = "/opt/discord-exporter/DiscordChatExporter.Cli"
	cfg.Tessdata = "/opt/tessdata"
	cfg.PDFDiff = "/usr/local/bin/diff-pdf"
	checksum, err := os.ReadFile("/opt/pdf-diff.sha256")
	if err != nil {
		return err
	}
	cfg.PDFDiffSHA = strings.TrimSpace(string(checksum))
	return nil
}
