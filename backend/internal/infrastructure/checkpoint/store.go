// Package checkpoint implements cold, filesystem-level recovery. Its caller must
// hold the operation lock and stop every writer before calling Create or Restore.
package checkpoint

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"tree-eclass/internal/domain/platform"
)

var validID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}$`)

// Manifest covers the entire cold dataset, not just relational rows or object keys.
type Manifest struct {
	ID          string            `json:"id"`
	Development string            `json:"development_baseline,omitempty"`
	Created     time.Time         `json:"created"`
	Release     string            `json:"release"`
	Reason      string            `json:"reason"`
	Versions    map[string]string `json:"versions"`
	Files       []File            `json:"files"`
}
type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	SHA256 string `json:"sha256,omitempty"`
}
type Store struct {
	Root string
	// Stopped must fail if a managed or unmanaged writer could still own the data.
	Stopped func() error
	// Step is a fault-injection boundary, called only after durable journal writes.
	Step func(string) error
}

type restoreJournal struct {
	Snapshot string `json:"snapshot"`
	Phase    string `json:"phase"`
}

func ID() string               { return time.Now().UTC().Format("20060102T150405") + "-" + rand.Text()[:10] }
func (s Store) Active() string { return filepath.Join(s.Root, "active") }
func (s Store) path(id string) (string, error) {
	if !validID.MatchString(id) {
		return "", errors.New("invalid checkpoint ID")
	}
	return filepath.Join(s.Root, "checkpoints", id), nil
}
func (s Store) stopped() error {
	if s.Stopped == nil {
		return errors.New("checkpoint requires a verified writer shutdown")
	}
	return s.Stopped()
}
func (s Store) step(name string) error {
	if s.Step != nil {
		return s.Step(name)
	}
	return nil
}

func inventory(root string) ([]File, error) {
	files := []File{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !d.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported data entry %s", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		size := info.Size()
		if d.IsDir() {
			size = 0
		}
		hash := ""
		if !d.IsDir() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			h := sha256.New()
			_, err = io.Copy(h, f)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			hash = fmt.Sprintf("%x", h.Sum(nil))
		}
		files = append(files, File{rel, size, uint32(info.Mode()), hash})
		return nil
	})
	return files, err
}

func (s Store) Create(m Manifest) (Manifest, error) {
	if err := s.stopped(); err != nil {
		return m, err
	}
	if m.ID == "" {
		m.ID = ID()
	}
	path, err := s.path(m.ID)
	if err != nil {
		return m, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return m, err
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return m, fmt.Errorf("checkpoint already exists: %s", m.ID)
	}
	temp := path + ".incomplete"
	if err = os.Mkdir(temp, 0700); err != nil {
		return m, err
	}
	defer os.RemoveAll(temp)
	if err = platform.CloneTree(s.Active(), filepath.Join(temp, "data")); err != nil {
		return m, err
	}
	m.Files, err = inventory(filepath.Join(temp, "data"))
	if err != nil {
		return m, err
	}
	m.Created = time.Now().UTC()
	if err = platform.WriteJSON(filepath.Join(temp, "manifest.json"), m); err != nil {
		return m, err
	}
	if err = os.Rename(temp, path); err != nil {
		return m, err
	}
	return m, platform.SyncDir(filepath.Dir(path))
}

func (s Store) Read(id string) (Manifest, error) {
	var m Manifest
	path, err := s.path(id)
	if err != nil {
		return m, err
	}
	if err = platform.ReadJSON(filepath.Join(path, "manifest.json"), &m); err != nil {
		return m, err
	}
	if m.ID != id {
		return m, errors.New("checkpoint manifest identity mismatch")
	}
	actual, err := inventory(filepath.Join(path, "data"))
	if err != nil {
		return m, err
	}
	if len(actual) != len(m.Files) {
		return m, errors.New("checkpoint inventory differs")
	}
	for i := range actual {
		if actual[i] != m.Files[i] {
			return m, fmt.Errorf("checkpoint entry differs: %s", actual[i].Path)
		}
	}
	return m, nil
}

func (s Store) List() ([]Manifest, error) {
	entries, err := os.ReadDir(filepath.Join(s.Root, "checkpoints"))
	if errors.Is(err, os.ErrNotExist) {
		return []Manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := []Manifest{}
	for _, entry := range entries {
		if !entry.IsDir() || filepath.Ext(entry.Name()) == ".incomplete" {
			continue
		}
		m, err := s.Read(entry.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, nil
}
