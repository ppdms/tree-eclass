package workflow

import (
	"io/fs"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
)

// This is the complete Python runtime boundary. Business services, credentials,
// and PostgreSQL stores do not enter the release.
func copyParserSource(source, target string) error {
	files := []string{"parser/__init__.py", "parser/parser_helper.py", "parser/models.py",
		"parser/source_files.py", "parser/vision.py", "parser/archive_limits.py"}
	for _, dir := range []string{"parser/extractors", "parser/archive_members"} {
		if err := filepath.WalkDir(filepath.Join(source, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "__pycache__" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".py" {
				return nil
			}
			rel, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			files = append(files, rel)
			return nil
		}); err != nil {
			return err
		}
	}
	for _, rel := range files {
		out := filepath.Join(target, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0700); err != nil {
			return err
		}
		if err := platform.CloneFile(filepath.Join(source, rel), out); err != nil {
			return err
		}
	}
	return nil
}
func packageParserSource(root string) error {
	temp, err := os.MkdirTemp(root, ".parser-source-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	if err = copyParserSource(root, temp); err != nil {
		return err
	}
	if err = os.RemoveAll(filepath.Join(root, "parser")); err != nil {
		return err
	}
	return os.Rename(filepath.Join(temp, "parser"), filepath.Join(root, "parser"))
}
