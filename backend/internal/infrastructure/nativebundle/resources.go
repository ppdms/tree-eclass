package nativebundle

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"tree-eclass/internal/domain/platform"
)

func (g *graph) resources(n *node) (bool, error) {
	if !strings.HasPrefix(filepath.Base(n.source), "libpoppler.") {
		return false, nil
	}
	source := filepath.Join(filepath.Dir(filepath.Dir(n.source)), "share/poppler")
	if err := copyResources(source, filepath.Join(g.root, "share/poppler")); err != nil {
		return false, err
	}
	// Poppler embeds a C string for its data directory and has no environment
	// override. Keep its offset/size intact and make it relative to the release
	// working directory, used by both parser and visual-difference runners.
	data, err := os.ReadFile(n.target)
	if err != nil {
		return false, err
	}
	before := append([]byte(source), 0)
	after := []byte("runtime/share/poppler")
	if len(after) >= len(before) || bytes.Count(data, before) != 1 {
		return false, errors.New("unrecognized Poppler resource path; native packaging refused")
	}
	replacement := make([]byte, len(before))
	copy(replacement, after)
	data = bytes.Replace(data, before, replacement, 1)
	return true, os.WriteFile(n.target, data, 0700)
}
func copyResources(source, target string) error {
	return filepath.WalkDir(source, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, rel)
		if d.IsDir() {
			return os.Mkdir(dest, 0700)
		}
		return platform.CloneFile(path, dest)
	})
}
func fontConfiguration(root string) error {
	path := filepath.Join(root, "share/fontconfig")
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, "fonts.conf"), []byte(`<?xml version="1.0"?>
<!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">
<fontconfig>
  <dir>/System/Library/Fonts</dir>
  <dir>/Library/Fonts</dir>
  <cachedir prefix="xdg">fontconfig</cachedir>
  <alias><family>sans-serif</family><prefer><family>Helvetica</family></prefer></alias>
  <alias><family>serif</family><prefer><family>Times</family></prefer></alias>
  <alias><family>monospace</family><prefer><family>Courier</family></prefer></alias>
</fontconfig>
`), 0600)
}
