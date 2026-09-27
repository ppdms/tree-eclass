package mirror

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/blob"
)

func (s Service) writeObjects(
	ctx context.Context,
	target string,
	old manifest,
	objects map[string]File,
	reusable map[string]bool,
	result *Result,
) error {
	previous := manifestFiles(old)
	for _, relative := range sortedObjectKeys(objects) {
		if reusable[relative] {
			result.Unchanged++
			continue
		}
		destination := filepath.Join(target, filepath.FromSlash(relative))
		if _, exists := previous[relative]; !exists {
			if err := absent(destination); err != nil {
				return err
			}
		}
		if err := ensureParents(target, filepath.Dir(destination)); err != nil {
			return err
		}
		if err := s.copyObject(ctx, destination, *objects[relative].Object); err != nil {
			return err
		}
		if _, exists := previous[relative]; exists {
			result.Updated++
		} else {
			result.Added++
		}
		result.Bytes += objects[relative].Object.Bytes
	}
	return nil
}

func sortedObjectKeys(objects map[string]File) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func absent(destination string) error {
	if _, err := os.Lstat(destination); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("refusing to overwrite unowned eClass mirror file: %s", destination)
}

func ensureParents(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil || !filepath.IsLocal(relative) {
		return fmt.Errorf("eClass mirror destination escapes root: %s", target)
	}
	current := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if _, err = ensureChildDir(filepath.Dir(current), filepath.Base(current)); err != nil {
			return err
		}
	}
	return nil
}

func (s Service) copyObject(ctx context.Context, destination string, ref blob.Reference) error {
	body, err := s.Objects.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer body.Close()
	temp, err := os.CreateTemp(filepath.Dir(destination), ".tree-eclass-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err = temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	hash := sha256.New()
	count, copyErr := io.Copy(io.MultiWriter(temp, hash), io.LimitReader(body, ref.Bytes+1))
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if count != ref.Bytes || fmt.Sprintf("%x", hash.Sum(nil)) != ref.SHA256 {
		return errors.New("local eClass mirror object verification failed")
	}
	if err = os.Rename(tempName, destination); err != nil {
		return err
	}
	return platform.SyncDir(filepath.Dir(destination))
}

func checkRedirects(target string, old manifest, objects map[string]File, redirects map[string]string) error {
	previous := manifestFiles(old)
	for relative := range redirects {
		if _, wasFile := previous[relative]; wasFile {
			continue
		}
		if _, isObject := objects[relative]; isObject {
			return fmt.Errorf("duplicate local mirror path: %s", relative)
		}
		if err := absent(filepath.Join(target, filepath.FromSlash(relative))); err != nil {
			return err
		}
	}
	return nil
}

func removeStale(target string, old manifest, objects map[string]File, result *Result) error {
	previous := manifestFiles(old)
	for relative := range previous {
		if _, keep := objects[relative]; keep {
			continue
		}
		destination := filepath.Join(target, filepath.FromSlash(relative))
		info, err := os.Lstat(destination)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("local eClass mirror stale path is not a regular file: %s", destination)
		}
		if err = os.Remove(destination); err != nil {
			return err
		}
		result.Removed++
	}
	return nil
}

func writeSkeleton(target string, course Course, objects map[string]File, redirects map[string]string) error {
	saved := manifest{Format: format, CourseID: course.ID, CourseName: course.Name}
	for _, relative := range sortedObjectKeys(objects) {
		ref := objects[relative].Object
		if ref == nil {
			return errors.New("local eClass mirror received an empty object reference")
		}
		saved.Files = append(saved.Files, manifestFile{Path: relative, SHA256: ref.SHA256, Bytes: ref.Bytes})
	}
	for _, relative := range sortedRedirectKeys(redirects) {
		saved.Redirects = append(saved.Redirects, manifestLink{Path: relative, URL: redirects[relative]})
	}
	return platform.WriteJSON(filepath.Join(target, manifestName), saved)
}

func writeManifest(target string, course Course, objects map[string]File, redirects map[string]string) error {
	saved := manifest{Format: format, CourseID: course.ID, CourseName: course.Name}
	for _, relative := range sortedObjectKeys(objects) {
		info, err := os.Stat(filepath.Join(target, filepath.FromSlash(relative)))
		if err != nil {
			return err
		}
		ref := objects[relative].Object
		if ref == nil {
			return errors.New("local eClass mirror received an empty object reference")
		}
		saved.Files = append(saved.Files, manifestFile{
			Path: relative, SHA256: ref.SHA256, Bytes: ref.Bytes, ModifiedNS: info.ModTime().UnixNano(),
		})
	}
	for _, relative := range sortedRedirectKeys(redirects) {
		saved.Redirects = append(saved.Redirects, manifestLink{Path: relative, URL: redirects[relative]})
	}
	return platform.WriteJSON(filepath.Join(target, manifestName), saved)
}

func sortedRedirectKeys(redirects map[string]string) []string {
	keys := make([]string, 0, len(redirects))
	for relative := range redirects {
		keys = append(keys, relative)
	}
	sort.Strings(keys)
	return keys
}
