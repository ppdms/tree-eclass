package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func fixture(t *testing.T) Store {
	t.Helper()
	s := Store{Root: t.TempDir(), Stopped: func() error { return nil }}
	for _, dir := range []string{"postgres", "seaweed", "settings"} {
		if err := os.MkdirAll(filepath.Join(s.Active(), dir), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.Active(), dir, "content"), []byte("stable"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestInterruptedRestore(t *testing.T) {
	for _, phase := range []string{"prepare", "ready", "retired", "swapped"} {
		t.Run(phase, func(t *testing.T) {
			s := fixture(t)
			m, err := s.Create(Manifest{Reason: "development"})
			if err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{"postgres", "seaweed", "settings"} {
				if err = os.WriteFile(filepath.Join(s.Active(), dir, "content"), []byte("dev mutation"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err = os.WriteFile(filepath.Join(s.Active(), "dev-only"), []byte("discard"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Step = func(step string) error {
				if step == phase {
					return errors.New("simulated crash")
				}
				return nil
			}
			if err = s.Restore(m.ID); err == nil {
				t.Fatal("fault was not exercised")
			}
			s.Step = nil
			if err = s.Recover(); err != nil {
				t.Fatal(err)
			}
			if err = s.Recover(); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{"postgres", "seaweed", "settings"} {
				b, err := os.ReadFile(filepath.Join(s.Active(), dir, "content"))
				if err != nil || string(b) != "stable" {
					t.Fatalf("restore %s: %s %v", dir, b, err)
				}
			}
			if _, err = os.Stat(filepath.Join(s.Active(), "dev-only")); !os.IsNotExist(err) {
				t.Fatal("development data survived")
			}
			if _, err = s.Read(m.ID); err != nil {
				t.Fatal("checkpoint was changed", err)
			}
		})
	}
}

func TestCheckpointRefusesLiveWritersAndSymlinks(t *testing.T) {
	s := fixture(t)
	s.Stopped = func() error { return errors.New("writer owns database") }
	if _, err := s.Create(Manifest{}); err == nil {
		t.Fatal("live snapshot accepted")
	}
	s.Stopped = func() error { return nil }
	if err := os.Symlink(t.TempDir(), filepath.Join(s.Active(), "tablespace")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(Manifest{}); err == nil {
		t.Fatal("symlink accepted")
	}
	for _, id := range []string{"../escape", "/absolute", ".", ""} {
		if _, err := s.Read(id); err == nil {
			t.Fatalf("unsafe ID accepted %q", id)
		}
	}
}

func TestCorruptedCheckpointCannotReplaceActiveData(t *testing.T) {
	s := fixture(t)
	m, err := s.Create(Manifest{})
	if err != nil {
		t.Fatal(err)
	}
	path, _ := s.path(m.ID)
	if err = os.WriteFile(filepath.Join(path, "data", "postgres", "content"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Restore(m.ID); err == nil {
		t.Fatal("same-sized corruption was accepted")
	}
	data, err := os.ReadFile(filepath.Join(s.Active(), "postgres", "content"))
	if err != nil || string(data) != "stable" {
		t.Fatal("active data changed after rejected restore")
	}
}
