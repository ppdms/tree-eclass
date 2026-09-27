package workflow

import (
	"io/fs"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
)

// Copy immutable runtime dependencies using APFS clones, retaining validated
// relative symlinks. Checkpoint copying keeps its stricter no-symlinks contract.
func cloneArtifact(source, target string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.Mkdir(dest, 0700)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := releaseLink(source, path)
			if err != nil {
				return err
			}
			return os.Symlink(link, dest)
		}
		return platform.CloneFile(path, dest)
	})
}
