package workflow

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestReleaseSourceFromCommittedGitArchive(t *testing.T) {
	repo, output := t.TempDir(), t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if data, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, data)
		}
	}
	git("init", "-q")
	path := filepath.Join(repo, "source.txt")
	if err := os.WriteFile(path, []byte("committed source"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "source.txt")
	git(
		"-c",
		"commit.gpgsign=false",
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"-qm",
		"fixture",
	)
	if err := os.WriteFile(path, []byte("uncommitted change"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := archiveSource(t.Context(), repo, "HEAD", output); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(output, "source.txt"))
	if err != nil || string(data) != "committed source" {
		t.Fatal("archive did not preserve committed source", string(data), err)
	}
	entries, err := os.ReadDir(output)
	if err != nil || len(entries) != 1 {
		t.Fatal("Git archive metadata became a release file", entries, err)
	}
}

func TestReleasePromoteRejectsDirtyCheckoutBeforeShutdown(t *testing.T) {
	repo := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	run("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "source.txt"), []byte("committed"), 0600); err != nil {
		t.Fatal(err)
	}
	run("add", "source.txt")
	run(
		"-c",
		"commit.gpgsign=false",
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"-qm",
		"fixture",
	)
	if err := os.WriteFile(filepath.Join(repo, "source.txt"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Repo: repo, Config: Config{Format: 1}, State: Selection{Mode: "stable"}}
	before := c.State
	if err := c.releasePromote(t.Context()); err == nil {
		t.Fatal("dirty checkout accepted")
	}
	if c.State != before {
		t.Fatal("dirty preflight changed runtime state", c.State)
	}
}
