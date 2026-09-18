package workflow

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// sourceFiles includes embedded data and parser code, excluding dependency trees
// and generated output. A private copy gives the Go compiler a consistent input;
// edits arriving during the copy/build are detected before publication.
func sourceFiles(root string) ([]string, error) {
	files := []string{"backend/go.mod", "backend/go.sum"}
	for _, dir := range []string{"backend/cmd", "backend/internal", "parser"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "__pycache__" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, "_test.go") {
				return nil
			}
			switch filepath.Ext(path) {
			case ".go", ".sql", ".json", ".txt", ".py":
			default:
				return nil
			}
			if !d.Type().IsRegular() {
				return fmt.Errorf("development source must be a regular file: %s", path)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files = append(files, rel)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.Sort(files)
	return files, nil
}

func sourceDigest(root, copyTo string) (string, error) {
	files, err := sourceFiles(root)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, rel := range files {
		f, err := os.Open(filepath.Join(root, rel))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(hash, "%s\x00", rel)
		content := sha256.New()
		var out *os.File
		writer := io.Writer(content)
		if copyTo != "" {
			path := filepath.Join(copyTo, rel)
			if err = os.MkdirAll(filepath.Dir(path), 0700); err == nil {
				out, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			}
			if err != nil {
				f.Close()
				return "", err
			}
			writer = io.MultiWriter(content, out)
		}
		_, err = io.Copy(writer, f)
		f.Close()
		if out != nil {
			if closeErr := out.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			return "", err
		}
		fmt.Fprintf(hash, "%x\n", content.Sum(nil))
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
