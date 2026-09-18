// Package mirror materializes the current eClass and external catalogs as ordinary files.
// SeaweedFS remains authoritative; these trees are disposable local projections.
package mirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/text/unicode/norm"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/platform"
)

const (
	format       = 1
	reserveBytes = uint64(5 * 1024 * 1024 * 1024)
	lockName     = ".tree-eclass.lock"
	markerName   = ".tree-eclass-course.json"
	manifestName = ".tree-eclass-manifest.json"

	EClassSubtree   = "eclass"
	ExternalSubtree = "external"
)

type Reader interface {
	Open(context.Context, blob.Reference) (io.ReadCloser, error)
}

type File struct {
	Path     string
	Object   *blob.Reference
	Redirect string
}

type Course struct {
	ID     int64
	Name   string
	Hidden bool
}

type Result struct {
	Root      string
	Files     int
	Added     int
	Updated   int
	Removed   int
	Unchanged int
	Redirects int
	Bytes     int64
}

type Service struct {
	Root    string
	Objects Reader
}

type marker struct {
	Format   int    `json:"format"`
	CourseID int64  `json:"course_id"`
	Name     string `json:"name"`
}

type paths struct {
	CourseDir string
	Target    string
	Marker    string
}

func (s Service) Sync(
	ctx context.Context,
	course Course,
	subtree string,
	sourceRoot string,
	files []File,
) (Result, error) {
	var result Result
	if err := s.validate(subtree, sourceRoot); err != nil {
		return result, err
	}
	objects, redirects, err := desired(sourceRoot, files)
	if err != nil {
		return result, err
	}
	root, unlock, err := s.lockMirror()
	if err != nil {
		return result, err
	}
	defer unlock()
	targets, err := coursePaths(root, course, subtree)
	if err != nil {
		return result, err
	}
	old, tracked, err := readManifest(targets.Target, course)
	if err != nil {
		return result, err
	}
	if !tracked {
		if err = writeSkeleton(targets.Target, course, objects, redirects); err != nil {
			return result, err
		}
	}
	if err = s.apply(ctx, root, targets, course, old, objects, redirects, &result); err != nil {
		return result, err
	}
	result.Root = targets.Target
	result.Files = len(objects)
	result.Redirects = len(redirects)
	return result, nil
}

func (s Service) lockMirror() (string, func(), error) {
	root, err := filepath.Abs(s.Root)
	if err != nil {
		return "", nil, err
	}
	if root == string(filepath.Separator) {
		return "", nil, errors.New("local eClass mirror root cannot be the filesystem root")
	}
	if err = ensureDir(root); err != nil {
		return "", nil, err
	}
	lock, err := acquireLock(root)
	if err != nil {
		return "", nil, err
	}
	return root, func() { platform.Unlock(lock) }, nil
}

func (s Service) apply(
	ctx context.Context,
	root string,
	targets paths,
	course Course,
	old manifest,
	objects map[string]File,
	redirects map[string]string,
	result *Result,
) error {
	reusable, required, err := s.plan(ctx, targets.Target, old, objects)
	if err != nil {
		return err
	}
	if err = checkSpace(root, required); err != nil {
		return err
	}
	if err = s.writeObjects(ctx, targets.Target, old, objects, reusable, result); err != nil {
		return err
	}
	if err = checkRedirects(targets.Target, old, objects, redirects); err != nil {
		return err
	}
	if err = removeStale(targets.Target, old, objects, result); err != nil {
		return err
	}
	if err = writeManifest(targets.Target, course, objects, redirects); err != nil {
		return err
	}
	return platform.WriteJSON(
		targets.Marker,
		marker{Format: format, CourseID: course.ID, Name: course.Name},
	)
}

func (s Service) validate(subtree, sourceRoot string) error {
	if s.Root == "" || s.Objects == nil {
		return errors.New("local eClass mirror is not configured")
	}
	if subtree != EClassSubtree && subtree != ExternalSubtree {
		return fmt.Errorf("unsupported local mirror subtree: %s", subtree)
	}
	if sourceRoot == "" || path.Clean(sourceRoot) == "." {
		return errors.New("synchronized course root is required")
	}
	return nil
}

func acquireLock(root string) (*os.File, error) {
	lockPath := filepath.Join(root, lockName)
	if info, err := os.Lstat(lockPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local eClass mirror lock is a symlink")
	}
	return platform.Lock(lockPath)
}

func ensureDir(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(target, 0700)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("local eClass mirror root is not a directory: %s", target)
	}
	return nil
}

func ensureChildDir(parent, name string) (string, error) {
	target := filepath.Join(parent, name)
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return target, os.Mkdir(target, 0700)
	}
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("local eClass mirror path is not a directory: %s", target)
	}
	return target, nil
}

func coursePaths(root string, course Course, subtree string) (paths, error) {
	parent := root
	var err error
	if course.Hidden {
		parent, err = ensureChildDir(root, ".hidden")
		if err != nil {
			return paths{}, err
		}
	}
	name, err := courseComponent(course.Name)
	if err != nil {
		return paths{}, err
	}
	courseDir, err := ensureChildDir(parent, name)
	if err != nil {
		return paths{}, err
	}
	markerPath := filepath.Join(courseDir, markerName)
	if err = validateMarker(markerPath, course, courseDir, subtree); err != nil {
		return paths{}, err
	}
	target, err := ensureChildDir(courseDir, subtree)
	if err != nil {
		return paths{}, err
	}
	return paths{CourseDir: courseDir, Target: target, Marker: markerPath}, nil
}

func courseComponent(raw string) (string, error) {
	value := strings.Join(strings.Fields(norm.NFC.String(raw)), " ")
	value = strings.NewReplacer("/", "-", "\\", "-").Replace(value)
	value = strings.Trim(value, " .")
	return materials.Filename(value)
}

func relativePath(root, value string) (string, error) {
	relative, err := filepath.Rel(filepath.FromSlash(path.Clean(root)), filepath.FromSlash(path.Clean(value)))
	if err != nil {
		return "", fmt.Errorf("eClass file escapes synchronized root: %s", value)
	}
	relative = filepath.ToSlash(relative)
	if relative == "." || strings.HasPrefix(relative, "../") || strings.HasPrefix(relative, "/") {
		return "", fmt.Errorf("eClass file escapes synchronized root: %s", value)
	}
	if err = validRelative(relative); err != nil {
		return "", err
	}
	return relative, nil
}

func validRelative(value string) error {
	if value == "" || strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') ||
		!filepath.IsLocal(filepath.FromSlash(value)) {
		return fmt.Errorf("unsafe local eClass mirror path: %s", value)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." || part == manifestName {
			return fmt.Errorf("unsafe local eClass mirror path: %s", value)
		}
	}
	return nil
}
