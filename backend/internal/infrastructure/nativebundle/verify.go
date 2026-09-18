package nativebundle

import (
	"debug/macho"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
)

func Verify(root string) error {
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	for _, dir := range []string{"bin", "lib"} {
		if err = filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			file, err := macho.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			libraries, err := imports(file)
			if err != nil {
				return err
			}
			for _, library := range libraries {
				if system(library) {
					continue
				}
				if !strings.HasPrefix(library, "@loader_path/") {
					return errors.New("native dependency is not relative to its packaged loader")
				}
				resolved, err := filepath.EvalSymlinks(expand(library, path, path))
				if err != nil {
					return err
				}
				rel, err := filepath.Rel(canonical, resolved)
				if err != nil || !filepath.IsLocal(rel) {
					return errors.New("native dependency escapes release")
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
