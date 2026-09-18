package workflow

import (
	"archive/tar"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func extractPython(archive *tar.Reader, root string) error {
	var total int64
	for count := 0; count < 20000; count++ {
		h, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(h.Name)
		if !filepath.IsLocal(name) || (name != "python" && !strings.HasPrefix(name, "python/")) {
			return errors.New("unsafe Python archive path")
		}
		total += h.Size
		if h.Size < 0 || total > 400<<20 {
			return errors.New("Python archive exceeds expanded size limit")
		}
		path := filepath.Join(root, name)
		if err = regularParents(root, filepath.Dir(path)); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0777)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(f, archive)
			closeErr := f.Close()
			if err = errors.Join(copyErr, closeErr); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if filepath.IsAbs(h.Linkname) {
				return errors.New("absolute Python archive link")
			}
			target := filepath.Clean(filepath.Join(filepath.Dir(name), h.Linkname))
			if !strings.HasPrefix(target, "python/") {
				return errors.New("Python archive link escapes distribution")
			}
			if err = os.Symlink(h.Linkname, path); err != nil {
				return err
			}
		default:
			return errors.New("unsupported Python archive entry")
		}
	}
	return errors.New("Python archive exceeds file limit")
}

// Never let an earlier archive symlink redirect a later regular-file write.
func regularParents(root, parent string) error {
	rel, err := filepath.Rel(root, parent)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	if !filepath.IsLocal(rel) {
		return errors.New("archive parent escapes extraction root")
	}
	path := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			if err = os.Mkdir(path, 0700); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("archive parent is not a real directory")
		}
	}
	return nil
}
