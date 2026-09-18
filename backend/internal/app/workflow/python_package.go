package workflow

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func (c *Controller) packagePython(ctx context.Context, root string) error {
	base, err := c.prepareParser(ctx, filepath.Join(root, "requirements-parser.txt"))
	if err != nil {
		return err
	}
	return cloneArtifact(filepath.Join(base, "python"), filepath.Join(root, ".python"))
}

// Runtime invokes the interpreter directly. Installed command wrappers can
// contain the temporary build prefix in their shebang, so do not package them.
func trimPython(root string) error {
	entries, err := os.ReadDir(filepath.Join(root, "bin"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "python3.14" || entry.Name() == "python3" || entry.Name() == "python" {
			continue
		}
		if err = os.RemoveAll(filepath.Join(root, "bin", entry.Name())); err != nil {
			return err
		}
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "__pycache__" {
			if err = os.RemoveAll(path); err != nil {
				return err
			}
			return filepath.SkipDir
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".pyc") {
			return os.Remove(path)
		}
		return nil
	})
}

func pythonPath(root, mode string) string {
	if mode == "stable" {
		return filepath.Join(root, ".python/bin/python3.14")
	}
	return filepath.Join(root, ".venv/bin/python")
}

func ensurePythonExecutable(root string) error {
	info, err := os.Stat(filepath.Join(root, "bin/python3.14"))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("bundled Python interpreter is not executable")
	}
	return nil
}
