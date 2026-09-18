package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/platform"
)

func TestReleaseRetentionProtectsCodeNeededByData(t *testing.T) {
	c := &Controller{Root: t.TempDir()}
	root := filepath.Join(c.Root, "releases")
	ids := []string{}
	for _, digit := range []string{"1", "2", "3", "4", "5", "6"} {
		id := strings.Repeat(digit, 40)
		ids = append(ids, id)
		path := filepath.Join(root, id)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := platform.WriteJSON(filepath.Join(path, "release.json"), Release{ID: id, Commit: id}); err != nil {
			t.Fatal(err)
		}
	}
	c.State.Release, c.State.Previous = ids[0], ids[1]
	c.releasePin = ids[5]
	if err := platform.WriteJSON(filepath.Join(root, "latest-build.json"), latestBuild{Release: ids[2]}); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(root, "unrecognized")
	if err := os.Mkdir(unknown, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.pruneReleases([]checkpoint.Manifest{{ID: "recovery", Release: ids[3]}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range append(ids[:4:4], ids[5]) {
		if _, err := os.Stat(filepath.Join(root, id)); err != nil {
			t.Fatal("removed protected release", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, ids[4])); !os.IsNotExist(err) {
		t.Fatal("obsolete artifact retained", err)
	}
	if _, err := os.Stat(unknown); err != nil {
		t.Fatal("unrecognized directory removed", err)
	}
}
func TestReleaseRetentionRefusesUnrecognizedManifestBeforeDeleting(t *testing.T) {
	c := &Controller{Root: t.TempDir()}
	root := filepath.Join(c.Root, "releases")
	first, last := strings.Repeat("1", 40), strings.Repeat("9", 40)
	for _, id := range []string{first, last} {
		path := filepath.Join(root, id)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		manifest := Release{ID: id, Commit: id}
		if id == last {
			manifest.Commit = "wrong"
		}
		if err := platform.WriteJSON(filepath.Join(path, "release.json"), manifest); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.pruneReleases(nil); err == nil {
		t.Fatal("mismatched identity accepted")
	}
	if _, err := os.Stat(filepath.Join(root, first)); err != nil {
		t.Fatal("partial cleanup before inspection completed", err)
	}
}
