package rdbms_test

import (
	"path/filepath"
	"testing"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/rdbms"
)

type contractBackend struct {
	name string
	cfg  rdbms.Config
}

func contractBackends(t *testing.T) []contractBackend {
	t.Helper()
	return []contractBackend{{name: "sqlite", cfg: rdbms.Config{
		SQLitePath: filepath.Join(t.TempDir(), "contract.db"),
	}}}
}

func openContractStore(t *testing.T, cfg rdbms.Config) database.Store {
	t.Helper()
	store, err := rdbms.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}
