package workflow

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"tree-eclass/internal/domain/platform"
)

// Exercise the real shell launcher and Go command dispatch, with the test
// executable acting as the installed controller. Every path remains disposable.
func fixtureCLI(t *testing.T, c *Controller, args ...string) {
	t.Helper()
	home := t.TempDir()
	if err := os.Symlink(c.Root, filepath.Join(home, "native-v1")); err != nil {
		t.Fatal(err)
	}
	launcher, err := os.ReadFile("../../../../tree")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.Repo, "tree")
	if err = os.WriteFile(path, launcher, 0700); err != nil {
		t.Fatal(err)
	}
	if err = InstallController(c.Executable, filepath.Join(c.Root, "controller")); err != nil {
		t.Fatal(err)
	}
	if err = platform.WriteJSON(filepath.Join(c.Root, "config.json"), c.Config); err != nil {
		t.Fatal(err)
	}
	if err = c.save(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), path, args...)
	// Go is deliberately absent: recovery must not need a compiler or editable module.
	cmd.Env = []string{
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"HOME=" + home,
		"TREE_WORKFLOW_HOME=" + home,
		"TREE_NATIVE_TESTS=1",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native CLI %v: %s %v", args, out, err)
	}
	c.State = Selection{}
	if err = platform.ReadJSON(filepath.Join(c.Root, "selection.json"), &c.State); err != nil {
		t.Fatal(err)
	}
}
