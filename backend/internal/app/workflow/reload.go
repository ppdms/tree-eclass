package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/platform"
)

func binaryManifest(ctx context.Context, binary string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "manifest")
	output := &manifestOutput{}
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	b := output.Bytes()
	var manifest map[string]string
	if len(b) > 256*1024 {
		return nil, errors.New("migration manifest exceeds limit")
	}
	err := json.Unmarshal(b, &manifest)
	if err == nil && len(manifest) == 0 {
		err = errors.New("empty migration manifest")
	}
	return manifest, err
}

func compatibleManifest(old, next map[string]string) error {
	for name, hash := range old {
		if next[name] != hash {
			return fmt.Errorf("applied migration %s was changed or removed; append a new migration", name)
		}
	}
	return nil
}

func (c *Controller) replaceDevelopment(ctx context.Context, candidate, code string) (err error) {
	binary := filepath.Join(c.Root, "build/tree-eclass")
	old, err := binaryManifest(ctx, binary)
	if err != nil {
		return err
	}
	next, err := binaryManifest(ctx, candidate)
	if err != nil {
		return err
	}
	if err = compatibleManifest(old, next); err != nil {
		return err
	}
	var previous server.Config
	if err = platform.ReadJSON(filepath.Join(c.Root, "run/application.json"), &previous); err != nil {
		return err
	}
	schemaChange := !maps.Equal(old, next)
	frontendStopped := !c.Processes.Alive("frontend-build")
	if schemaChange {
		if err = c.prepareDevelopmentMigration(ctx, candidate); err != nil {
			return err
		}
	} else if err = c.stopAPI(); err != nil {
		return err
	}
	backup := binary + ".previous"
	if err = os.Rename(binary, backup); err != nil {
		return err
	}
	if err = os.Rename(candidate, binary); err != nil {
		return errors.Join(err, os.Rename(backup, binary))
	}
	if err = platform.SyncDir(filepath.Dir(binary)); err != nil {
		return err
	}
	if err = c.startAPI(ctx, c.Repo, binary, code, false); err != nil {
		return c.fallbackDevelopment(ctx, binary, backup, previous, err, schemaChange, frontendStopped)
	}
	if schemaChange || frontendStopped {
		if err = c.startFrontendBuild(ctx, c.Repo); err != nil {
			return errors.Join(err, c.stopDataset())
		}
	}
	return os.Remove(backup)
}

// fallbackDevelopment rolls back a candidate that failed readiness. Compilation
// and schema compatibility already succeeded, so the previous code restarts
// without restoring any dataset writes.
func (c *Controller) fallbackDevelopment(
	ctx context.Context,
	binary, backup string,
	previous server.Config,
	cause error,
	schemaChange, frontendStopped bool,
) error {
	if schemaChange {
		return errors.Join(cause, c.stopDataset())
	}
	err := errors.Join(cause, c.stopAPI())
	if restoreErr := os.Rename(backup, binary); restoreErr != nil {
		return errors.Join(err, restoreErr)
	}
	fallbackErr := c.startAPI(ctx, c.Repo, binary, previous.Code, false)
	if fallbackErr == nil && frontendStopped {
		fallbackErr = c.startFrontendBuild(ctx, c.Repo)
	}
	return errors.Join(err, fallbackErr)
}

func (c *Controller) prepareDevelopmentMigration(ctx context.Context, candidate string) (err error) {
	if err = c.stopDataset(); err != nil {
		return err
	}
	if err = c.space(); err != nil {
		return err
	}
	snapshots := checkpoint.Store{Root: c.Root, Stopped: c.datasetStopped}
	m, err := snapshots.Create(
		checkpoint.Manifest{
			Release:     c.State.Release,
			Reason:      "development-schema",
			Development: c.State.Baseline,
			Versions:    c.versions(),
		},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Development schema checkpoint %s created before migration.\n", m.ID)
	if err = c.infrastructure(ctx); err == nil {
		err = c.migrateBinary(ctx, candidate)
	}
	if err != nil {
		fmt.Println(
			"Migration failed; application stopped. Fix the migration and retry, or exit development to discard its data. No checkpoint was silently restored.",
		)
		return errors.Join(err, c.stopDataset())
	}
	return nil
}

type manifestOutput struct{ bytes.Buffer }

func (w *manifestOutput) Write(p []byte) (int, error) {
	if w.Len()+len(p) > 256*1024 {
		return 0, errors.New("migration manifest exceeds limit")
	}
	return w.Buffer.Write(p)
}
