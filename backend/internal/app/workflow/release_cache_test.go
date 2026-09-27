package workflow

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestBuildEnvironmentOmitsRuntimeFixtureVariables(t *testing.T) {
	for _, name := range []string{
		"TREE_NATIVE_TESTS", "TREE_NATIVE_TOOLS_TEST", "TREE_RUNTIME_CONFIG",
		"TREE_FIXTURE_APPLICATION",
	} {
		t.Setenv(name, "fixture")
	}
	for _, value := range (&Controller{Root: t.TempDir()}).buildEnvironment() {
		name, _, _ := strings.Cut(value, "=")
		if name == "TREE_NATIVE_TESTS" || name == "TREE_NATIVE_TOOLS_TEST" ||
			name == "TREE_RUNTIME_CONFIG" || name == "TREE_FIXTURE_APPLICATION" ||
			strings.HasPrefix(name, "TREE_TEST_") {
			t.Fatal("runtime fixture variable leaked into build environment", name)
		}
	}
}

func TestReleaseCacheCleanupPreservesActiveDevelopment(t *testing.T) {
	c := &Controller{Root: t.TempDir()}
	live := []string{"build/tree-eclass", "run/jobs/active-parser", "cache/go-build/live"}
	for _, name := range live {
		path := filepath.Join(c.Root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cleanup := c.releaseBuildCache()
	root := filepath.Join(c.Root, "cache/release")
	if !slices.Contains(c.buildEnvironment(), "GOMODCACHE="+filepath.Join(root, "go-mod")) {
		t.Fatal("release inherited editable build cache")
	}
	module := filepath.Join(root, "go-mod/pkg")
	if err := os.MkdirAll(module, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(module, "data"), []byte("disposable"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(module, 0500); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("release cache retained", err)
	}
	for _, name := range live {
		if _, err := os.Stat(filepath.Join(c.Root, name)); err != nil {
			t.Fatal("active development output removed", name, err)
		}
	}
	if c.buildCache != "" {
		t.Fatal("release cache override leaked")
	}
}
