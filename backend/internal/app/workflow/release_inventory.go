package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func releaseFiles(root string) (map[string]string, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("release root must be a real directory")
	}
	files := map[string]string{}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "release.json" {
			if !d.Type().IsRegular() {
				return errors.New("release manifest must be a regular file")
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, err := releaseLink(root, path)
			if err != nil {
				return err
			}
			files[rel] = "symlink:" + target
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unsupported release artifact: %s", rel)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		hash := sha256.New()
		if _, err = io.Copy(hash, f); err != nil {
			return err
		}
		files[rel] = hex.EncodeToString(hash.Sum(nil))
		return nil
	})
	return files, err
}
func verifyRelease(root string, release Release) error {
	files, err := releaseFiles(root)
	if err != nil {
		return err
	}
	if len(files) != len(release.Files) {
		return errors.New("release file inventory changed")
	}
	for name, hash := range files {
		if release.Files[name] != hash {
			return fmt.Errorf("release artifact changed: %s", name)
		}
	}
	return nil
}

// Dependency links must remain entirely inside the published directory after
// its build directory is renamed. Record link identity as well as file bytes.
func releaseLink(root, path string) (string, error) {
	target, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(target) {
		return "", errors.New("release contains an absolute dependency symlink")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(canonicalRoot, resolved)
	if err != nil {
		return "", err
	}
	if !filepath.IsLocal(relative) {
		return "", errors.New("release dependency symlink escapes its artifact directory")
	}
	return target, nil
}
