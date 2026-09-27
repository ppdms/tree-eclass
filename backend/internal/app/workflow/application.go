package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/platform"
)

func (c *Controller) application(ctx context.Context) error {
	root := c.Repo
	code := c.State.Release
	binary := filepath.Join(c.Root, "build", "tree-eclass")
	if c.State.Mode == "stable" {
		root = filepath.Join(c.Root, "releases", c.State.Release)
		binary = filepath.Join(root, "tree-eclass")
	} else {
		var err error
		code, err = c.buildDevelopment(ctx, binary)
		if err != nil {
			return err
		}
	}
	if c.State.Mode == "development" {
		if err := c.startFrontendBuild(ctx, root); err != nil {
			return err
		}
	}
	if err := c.startAPI(ctx, root, binary, code, true); err != nil {
		return err
	}
	return nil
}

func (c *Controller) startAPI(ctx context.Context, root, binary, code string, migrate bool) error {
	if c.Processes.Alive("api") {
		return errors.New("API is already running")
	}
	if err := c.stopAPI(); err != nil {
		return err
	}
	temp := filepath.Join(c.Root, "run", "jobs")
	if err := os.MkdirAll(temp, 0700); err != nil {
		return err
	}
	cfg, err := c.apiConfig(root, code, temp)
	if err != nil {
		return err
	}
	keys, err := c.providerKeys()
	if err != nil {
		return err
	}
	cfg.ProviderKeys = keys
	configPath := filepath.Join(c.Root, "run", "application.json")
	if err := platform.WriteJSON(configPath, cfg); err != nil {
		return err
	}
	env := []string{"TREE_RUNTIME_CONFIG=" + configPath, "GOMEMLIMIT=192MiB"}
	if migrate {
		if err := c.runMigration(ctx, binary, env); err != nil {
			return err
		}
	}
	if err := c.start(ctx, "api", []string{binary}, env, 0); err != nil {
		return err
	}
	return c.waitService(ctx, "api", fmt.Sprintf("http://127.0.0.1:%d/api/health", c.Config.Ports.HTTP))
}

func (c *Controller) apiConfig(root, code, temp string) (server.Config, error) {
	cfg := server.Config{
		DatabaseURL: c.databaseURL(), ObjectsRoot: c.objectsRoot(),
		Address: fmt.Sprintf("127.0.0.1:%d", c.Config.Ports.HTTP),
		Mode:    c.State.Mode, Release: c.State.Release, Session: c.State.Session,
		Code: code, Temp: temp, ParserPython: pythonPath(root, c.State.Mode),
		ParserRoot: root, Tessdata: c.Config.Tessdata,
		DiscordExporter: c.Config.DiscordExporter, PDFDiff: c.Config.PDFDiff,
		PDFDiffSHA: c.Config.PDFDiffSHA, ExternalWorkers: c.State.Mode == "stable",
	}
	if err := c.configureAPI(root, &cfg); err != nil {
		return server.Config{}, err
	}
	return cfg, nil
}

func (c *Controller) configureAPI(root string, cfg *server.Config) error {
	if c.State.Mode == "development" && c.Config.ParserPython != "" {
		base, err := c.preparedParser()
		if err != nil {
			return fmt.Errorf("verify development parser dependencies; run ./tree down then ./tree setup: %w", err)
		}
		cfg.ParserPython = filepath.Join(base, "python/bin/python3.14")
	}
	cfg.FrontendDir = filepath.Join(c.Root, "build/frontend")
	if c.State.Mode == "development" {
		cfg.StorageNamespace = "tree-eclass:development:" + c.State.Baseline + ":"
	}
	if c.State.Mode == "stable" {
		cfg.FrontendDir = filepath.Join(root, "frontend")
		if err := applyRuntime(cfg, root); err != nil {
			return err
		}
	}
	return verifyTessdata(cfg.Tessdata)
}

func (c *Controller) waitService(ctx context.Context, name, url string) error {
	deadline := time.Now().Add(60 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	for {
		if !c.Processes.Alive(name) {
			return fmt.Errorf("%s stopped before readiness; see ./tree logs %s", name, name)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service did not become ready at %s", url)
		}
		retry.Reset(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry.C:
		}
	}
}
