package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/process"
)

func TestProviderSettingsBelongToTheDataset(t *testing.T) {
	c := &Controller{Root: t.TempDir()}
	c.Processes = process.Manager{Root: filepath.Join(c.Root, "processes")}
	if err := os.MkdirAll(filepath.Join(c.active(), "settings"), 0700); err != nil {
		t.Fatal(err)
	}
	imports := 0
	load := func(context.Context) (map[string]string, error) {
		imports++
		return map[string]string{"SYNTHETIC_API_KEY": "stable-key"}, nil
	}
	if err := c.ensureProviderKeysFrom(t.Context(), load); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SYNTHETIC_API_KEY", "ambient-key-must-not-replace-snapshot")
	if err := c.ensureProviderKeysFrom(t.Context(), load); err != nil || imports != 1 {
		t.Fatal("reimported provider credentials", err)
	}
	keys, err := c.providerKeys()
	if err != nil || keys["SYNTHETIC_API_KEY"] != "stable-key" {
		t.Fatal("ambient key escaped snapshot", err)
	}
	snapshots := checkpoint.Store{Root: c.Root, Stopped: func() error { return nil }}
	baseline, err := snapshots.Create(checkpoint.Manifest{Reason: "development"})
	if err != nil {
		t.Fatal(err)
	}
	c.State.Baseline = baseline.ID
	if err = platform.WriteJSON(c.providerPath(), providerSettings{1, map[string]string{"SYNTHETIC_API_KEY": "development-key"}}); err != nil {
		t.Fatal(err)
	}
	if err = c.importProviderKeysFrom(t.Context(), load); err == nil || imports != 1 {
		t.Fatal("development changed external credential source")
	}
	if err = snapshots.Restore(baseline.ID); err != nil {
		t.Fatal(err)
	}
	keys, err = c.providerKeys()
	if err != nil || keys["SYNTHETIC_API_KEY"] != "stable-key" {
		t.Fatal("provider credentials did not restore", err)
	}
	if err = os.Remove(c.providerPath()); err != nil {
		t.Fatal(err)
	}
	if err = c.ensureProviderKeysFrom(t.Context(), load); err == nil || imports != 1 {
		t.Fatal("missing development credentials silently imported stable keys")
	}
}

func TestInvalidProviderImportPreservesExistingKeys(t *testing.T) {
	c := &Controller{Root: t.TempDir()}
	c.Processes = process.Manager{Root: filepath.Join(c.Root, "processes")}
	if err := os.MkdirAll(filepath.Dir(c.providerPath()), 0700); err != nil {
		t.Fatal(err)
	}
	if err := platform.WriteJSON(c.providerPath(), providerSettings{1, map[string]string{"HF_TOKEN": "stable-key"}}); err != nil {
		t.Fatal(err)
	}
	bad := func(context.Context) (map[string]string, error) {
		return map[string]string{"DATABASE_URL": "must-not-import"}, nil
	}
	if err := c.importProviderKeysFrom(t.Context(), bad); err == nil {
		t.Fatal("non-provider settings imported")
	}
	keys, err := c.providerKeys()
	if err != nil || keys["HF_TOKEN"] != "stable-key" {
		t.Fatal("invalid import overwrote credentials")
	}
	if err = os.Chmod(c.providerPath(), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = c.providerKeys(); err == nil {
		t.Fatal("public credential file accepted")
	}
	if err = c.ensureProviderKeysFrom(t.Context(), bad); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid private file treated as absent")
	}
}
