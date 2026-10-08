package process

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestChildOverrideReplacesParentEntry(t *testing.T) {
	m := manager(t)
	marker := filepath.Join(t.TempDir(), "seen.env")
	t.Setenv("TREE_OVERRIDE_FIXTURE", "parent-value")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := Spec{
		Name:  "migration",
		Token: time.Now().String(),
		Env:   []string{"TREE_OVERRIDE_FIXTURE=child-value"},
		Dir:   filepath.Dir(marker),
		Command: fixtureCommand(
			t,
			"env-write",
			"TREE_OVERRIDE_FIXTURE",
			marker,
		),
	}
	if err := m.Run(ctx, spec); err != nil {
		t.Fatal(err)
	}
	seen, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(seen) != "child-value" {
		t.Fatalf("override lost, child saw %q", seen)
	}
}
