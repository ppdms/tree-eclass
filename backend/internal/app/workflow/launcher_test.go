package workflow

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/infrastructure/platform"
)

func TestLauncherRecoveryDoesNotCompileEditableCode(t *testing.T) {
	repo := filepath.Join(t.TempDir(), "editable source")
	root := filepath.Join(t.TempDir(), "private data")
	if err := os.MkdirAll(repo, 0700); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	launcher, err := os.ReadFile("../../../../tree")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(repo, "tree"), launcher, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(root, "native-v1"), 0700); err != nil {
		t.Fatal(err)
	}
	controller := filepath.Join(root, "native-v1/controller")
	if err = os.WriteFile(controller, []byte("#!/bin/sh\nprintf '%s\\n' \"$TREE_SOURCE_ROOT\" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"down"}, {"status"}, {"mcp"}, {"dev", "down"}} {
		cmd := exec.Command(filepath.Join(repo, "tree"), args...)
		cmd.Env = []string{"PATH=/usr/bin:/bin", "TREE_WORKFLOW_HOME=" + root, "HOME=" + t.TempDir()}
		out, err := cmd.CombinedOutput()
		if err != nil || string(out) != repo+"\n"+strings.Join(args, "\n")+"\n" {
			t.Fatalf("launcher %v: %s %v", args, out, err)
		}
	}
	cmd := exec.Command(filepath.Join(repo, "tree"), "controller", "update")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "TREE_WORKFLOW_HOME=" + root, "HOME=" + t.TempDir()}
	if err = cmd.Run(); err == nil {
		t.Fatal("missing compiler accepted")
	}
	if _, err = os.Stat(controller); err != nil {
		t.Fatal("failed update lost recovery controller", err)
	}
}

func TestControllerInstallRejectsUnfinishedModeSwitch(t *testing.T) {
	c, err := openAt(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, state := range []Selection{{Mode: "development", Baseline: "protected"}, {Mode: "stable"}, {Mode: "stopped", Baseline: "protected"}} {
		c.State = state
		if err = c.requireStoppedSetup(); err == nil {
			t.Fatal("unfinished selection accepted", state)
		}
	}
	c.State = Selection{Mode: "stopped"}
	if err = platform.WriteJSON(filepath.Join(c.Root, "activation.json"), activation{}); err != nil {
		t.Fatal(err)
	}
	if err = c.requireStoppedSetup(); err == nil {
		t.Fatal("pending activation accepted")
	}
}
