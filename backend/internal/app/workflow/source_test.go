package workflow

import (
	"os"
	"path/filepath"
	"testing"
)

func sourceFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"backend/cmd", "backend/internal/prompts", "parser", "frontend/node_modules", "parser/__pycache__"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{"backend/go.mod": "module fixture\n", "backend/go.sum": "", "backend/cmd/main.go": "package main", "backend/internal/prompts/rules.txt": "first", "parser/parser_helper.py": "x = 1", "frontend/node_modules/generated.js": "ignored", "parser/__pycache__/temp.py": "ignored"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestSourceIdentityIncludesEmbedsAndIsolatesCompilation(t *testing.T) {
	root := sourceFixture(t)
	copy := t.TempDir()
	first, err := sourceDigest(root, copy)
	if err != nil {
		t.Fatal(err)
	}
	if copied, err := sourceDigest(copy, ""); err != nil || copied != first {
		t.Fatal("copied input has a different identity", err)
	}
	if err = os.WriteFile(filepath.Join(root, "frontend/node_modules/generated.js"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if current, err := sourceDigest(root, ""); err != nil || current != first {
		t.Fatal("frontend dependency triggered Go reload", err)
	}
	if err = os.WriteFile(filepath.Join(root, "backend/internal/prompts/rules.txt"), []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	if current, err := sourceDigest(root, ""); err != nil || current == first {
		t.Fatal("embedded prompt edit ignored", err)
	}
	if copied, err := sourceDigest(copy, ""); err != nil || copied != first {
		t.Fatal("compiler input changed with working tree", err)
	}
	if err = os.Symlink("/etc/hosts", filepath.Join(root, "backend/internal/escape.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err = sourceDigest(root, ""); err == nil {
		t.Fatal("source symlink escaped snapshot")
	}
}
func TestReloadRejectsMigrationRewriteAndRemoval(t *testing.T) {
	old := map[string]string{"001.sql": "first"}
	for _, next := range []map[string]string{{}, {"001.sql": "changed"}} {
		if err := compatibleManifest(old, next); err == nil {
			t.Fatal("incompatible schema accepted")
		}
	}
	if err := compatibleManifest(old, map[string]string{"001.sql": "first", "002.sql": "second"}); err != nil {
		t.Fatal(err)
	}
}
