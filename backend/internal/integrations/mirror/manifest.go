package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/platform"
)

type manifest struct {
	Format     int            `json:"format"`
	CourseID   int64          `json:"course_id"`
	CourseName string         `json:"course_name"`
	Files      []manifestFile `json:"files"`
	Redirects  []manifestLink `json:"redirects"`
}

type manifestFile struct {
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	Bytes      int64  `json:"bytes"`
	ModifiedNS int64  `json:"modified_ns"`
}

type manifestLink struct {
	Path string `json:"path"`
	URL  string `json:"url"`
}

func desired(sourceRoot string, files []File) (map[string]File, map[string]string, error) {
	objects := map[string]File{}
	redirects := map[string]string{}
	for _, file := range files {
		relative, err := relativePath(sourceRoot, file.Path)
		if err != nil {
			return nil, nil, err
		}
		if file.Object == nil {
			if file.Redirect == "" {
				return nil, nil, fmt.Errorf("eClass file has no local object or redirect: %s", relative)
			}
			if _, exists := objects[relative]; exists {
				return nil, nil, fmt.Errorf("duplicate local mirror path: %s", relative)
			}
			if _, exists := redirects[relative]; exists {
				return nil, nil, fmt.Errorf("duplicate local mirror path: %s", relative)
			}
			redirects[relative] = file.Redirect
			continue
		}
		if _, exists := redirects[relative]; exists {
			return nil, nil, fmt.Errorf("duplicate local mirror path: %s", relative)
		}
		if _, exists := objects[relative]; exists {
			return nil, nil, fmt.Errorf("duplicate local mirror path: %s", relative)
		}
		objects[relative] = file
	}
	if err := validateDesiredPaths(objects, redirects); err != nil {
		return nil, nil, err
	}
	return objects, redirects, nil
}

func validateDesiredPaths(objects map[string]File, redirects map[string]string) error {
	paths := make([]string, 0, len(objects)+len(redirects))
	for relative := range objects {
		paths = append(paths, relative)
	}
	for relative := range redirects {
		paths = append(paths, relative)
	}
	sort.Strings(paths)
	for i := 1; i < len(paths); i++ {
		if strings.HasPrefix(paths[i], paths[i-1]+"/") {
			return fmt.Errorf("local mirror file conflicts with a directory: %s", paths[i-1])
		}
	}
	return nil
}

func validateMarker(name string, course Course, courseDir, subtree string) error {
	if err := rejectSymlink(name); err != nil {
		return err
	}
	var saved marker
	err := platform.ReadJSON(name, &saved)
	if errors.Is(err, os.ErrNotExist) {
		return createMarker(name, course, courseDir, subtree)
	}
	if err != nil {
		return err
	}
	if saved.Format != format || saved.CourseID != course.ID {
		return fmt.Errorf("unrecognized or mismatched eClass mirror marker: %s", name)
	}
	return nil
}

func createMarker(name string, course Course, courseDir, subtree string) error {
	target := filepath.Join(courseDir, subtree)
	info, err := os.Lstat(target)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("existing eClass mirror path is not a directory: %s", target)
		}
		entries, readErr := os.ReadDir(target)
		if readErr != nil {
			return readErr
		}
		if len(entries) != 0 {
			return fmt.Errorf("refusing to overwrite unowned eClass directory: %s", target)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return platform.WriteJSON(name, marker{Format: format, CourseID: course.ID, Name: course.Name})
}

func readManifest(target string, course Course) (manifest, bool, error) {
	name := filepath.Join(target, manifestName)
	if err := rejectSymlink(name); err != nil {
		return manifest{}, false, err
	}
	var saved manifest
	err := platform.ReadJSON(name, &saved)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(target)
		if readErr != nil {
			return manifest{}, false, readErr
		}
		if len(entries) != 0 {
			return manifest{}, false, fmt.Errorf("refusing to overwrite unowned eClass mirror directory: %s", target)
		}
		return manifest{Format: format, CourseID: course.ID, CourseName: course.Name}, false, nil
	}
	if err != nil {
		return manifest{}, false, err
	}
	if saved.Format != format || saved.CourseID != course.ID {
		return manifest{}, false, fmt.Errorf("unrecognized or mismatched eClass mirror manifest: %s", name)
	}
	if err = validateManifest(saved); err != nil {
		return manifest{}, false, err
	}
	return saved, true, nil
}

func rejectSymlink(name string) error {
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("local eClass mirror path is a symlink: %s", name)
	}
	return nil
}

func validateManifest(saved manifest) error {
	seen := map[string]bool{}
	for _, file := range saved.Files {
		if err := validRelative(file.Path); err != nil || !validHash(file.SHA256) || file.Bytes < 0 {
			return fmt.Errorf("invalid eClass mirror manifest file: %s", file.Path)
		}
		if seen[file.Path] {
			return fmt.Errorf("duplicate eClass mirror manifest path: %s", file.Path)
		}
		seen[file.Path] = true
	}
	for _, link := range saved.Redirects {
		if err := validRelative(link.Path); err != nil || link.URL == "" {
			return fmt.Errorf("invalid eClass mirror manifest redirect: %s", link.Path)
		}
		if seen[link.Path] {
			return fmt.Errorf("duplicate eClass mirror manifest path: %s", link.Path)
		}
		seen[link.Path] = true
	}
	return nil
}

func validHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s Service) plan(
	ctx context.Context,
	target string,
	old manifest,
	objects map[string]File,
) (map[string]bool, uint64, error) {
	previous := manifestFiles(old)
	reusable := map[string]bool{}
	var required uint64
	for relative, file := range objects {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		ref := file.Object
		if ref == nil {
			return nil, 0, errors.New("local eClass mirror received an empty object reference")
		}
		destination := filepath.Join(target, filepath.FromSlash(relative))
		same, err := existing(destination, previous[relative], *ref)
		if err != nil {
			return nil, 0, err
		}
		reusable[relative] = same
		if same {
			continue
		}
		if ref.Bytes < 0 || ref.Bytes > blob.MaxSourceBytes {
			return nil, 0, fmt.Errorf("local eClass mirror source exceeds limits: %s", relative)
		}
		if uint64(ref.Bytes) > ^uint64(0)-required {
			return nil, 0, errors.New("local eClass mirror size overflows")
		}
		required += uint64(ref.Bytes)
	}
	return reusable, required, nil
}

func checkSpace(root string, required uint64) error {
	free, err := platform.Available(root)
	if err != nil {
		return err
	}
	if required > ^uint64(0)-reserveBytes || free < reserveBytes+required {
		return errors.New("local eClass mirror paused: insufficient space for mirror plus 5 GiB reserve")
	}
	return nil
}

func manifestFiles(saved manifest) map[string]manifestFile {
	result := make(map[string]manifestFile, len(saved.Files))
	for _, file := range saved.Files {
		result[file.Path] = file
	}
	return result
}

func existing(destination string, old manifestFile, current blob.Reference) (bool, error) {
	info, err := os.Lstat(destination)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("local eClass mirror destination is not a regular file: %s", destination)
	}
	return old.Path != "" && old.SHA256 == current.SHA256 && old.Bytes == current.Bytes &&
		info.Size() == old.Bytes && info.ModTime().UnixNano() == old.ModifiedNS, nil
}
