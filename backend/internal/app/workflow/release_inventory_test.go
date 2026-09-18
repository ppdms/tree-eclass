package workflow

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReleaseInventoryBindsDependencyLinks(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "packages", "module")
	if err := os.MkdirAll(real, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "index.js"), []byte("fixed source"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "module")
	if err := os.Symlink("packages/module", link); err != nil {
		t.Fatal(err)
	}
	files, err := releaseFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	release := Release{Files: files}
	if err = verifyRelease(root, release); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("packages/../packages/module", link); err != nil {
		t.Fatal(err)
	}
	if err = verifyRelease(root, release); err == nil {
		t.Fatal("changed link identity accepted")
	}
	if err = os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err = releaseFiles(root); err == nil {
		t.Fatal("absolute link would bind the staging path")
	}
	if err = os.Remove(link); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	relative, err := filepath.Rel(root, outside)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(relative, link); err != nil {
		t.Fatal(err)
	}
	if _, err = releaseFiles(root); err == nil {
		t.Fatal("escaping dependency accepted")
	}
}
func TestReleaseInventoryRejectsBrokenAndRootLinks(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "real")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink("real", alias); err != nil {
		t.Fatal(err)
	}
	if _, err := releaseFiles(alias); err == nil {
		t.Fatal("linked release root accepted")
	}
	if err := os.Symlink("missing", filepath.Join(root, "broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := releaseFiles(root); err == nil {
		t.Fatal("broken dependency accepted")
	}
}
