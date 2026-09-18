package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"tree-eclass/internal/infrastructure/blob"
)

type fakeReader map[string][]byte

func (f fakeReader) Open(_ context.Context, ref blob.Reference) (io.ReadCloser, error) {
	data, ok := f[ref.SHA256]
	if !ok {
		return nil, errors.New("missing fake object")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func fakeReference(data []byte) blob.Reference {
	digest := sha256.Sum256(data)
	encoded := hex.EncodeToString(digest[:])
	return blob.Reference{
		Bucket:    blob.DataBucket,
		Key:       "objects/" + encoded,
		VersionID: "v1",
		SHA256:    encoded,
		Bytes:     int64(len(data)),
	}
}

func TestSyncCreatesAndUpdatesCourseMirror(t *testing.T) {
	first := []byte("first version")
	second := []byte("second version")
	reader := fakeReader{fakeReference(first).SHA256: first, fakeReference(second).SHA256: second}
	service := Service{Root: t.TempDir(), Objects: reader}
	course := Course{ID: 168, Name: "Λειτουργικά Συστήματα"}
	sourceRoot := "/Courses/168/eclass"
	files := []File{
		{Path: sourceRoot + "/week/notes.txt", Object: pointer(fakeReference(first))},
		{Path: sourceRoot + "/old.txt", Object: pointer(fakeReference(first))},
		{Path: sourceRoot + "/online.html", Redirect: "https://eclass.aueb.gr/online"},
	}
	result, err := service.Sync(context.Background(), course, EClassSubtree, sourceRoot, files)
	if err != nil || result.Added != 2 || result.Redirects != 1 {
		t.Fatalf("initial mirror: %#v %v", result, err)
	}
	target := filepath.Join(service.Root, course.Name, "eclass")
	if content, readErr := os.ReadFile(filepath.Join(target, "week", "notes.txt")); readErr != nil ||
		string(content) != string(first) {
		t.Fatalf("initial file: %q %v", content, readErr)
	}
	files = []File{
		{Path: sourceRoot + "/week/notes.txt", Object: pointer(fakeReference(second))},
		{Path: sourceRoot + "/online.html", Redirect: "https://eclass.aueb.gr/online"},
	}
	result, err = service.Sync(context.Background(), course, EClassSubtree, sourceRoot, files)
	if err != nil || result.Updated != 1 || result.Removed != 1 || result.Unchanged != 0 {
		t.Fatalf("updated mirror: %#v %v", result, err)
	}
	if _, err = os.Stat(filepath.Join(target, "old.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale file remains: %v", err)
	}
	if _, err = os.Stat(filepath.Join(target, "online.html")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("redirect became a local file: %v", err)
	}
	result, err = service.Sync(context.Background(), course, EClassSubtree, sourceRoot, files)
	if err != nil || result.Unchanged != 1 || result.Bytes != 0 {
		t.Fatalf("unchanged mirror: %#v %v", result, err)
	}
}

func TestSyncMirrorsExternalFilesInSeparateSubtree(t *testing.T) {
	body := []byte("past paper")
	reader := fakeReader{fakeReference(body).SHA256: body}
	service := Service{Root: t.TempDir(), Objects: reader}
	course := Course{ID: 169, Name: "Δίκτυα Υπολογιστών"}
	sourceRoot := "/Courses/169/external"
	files := []File{{
		Path: sourceRoot + "/past-papers/June.pdf", Object: pointer(fakeReference(body)),
	}}
	result, err := service.Sync(
		context.Background(), course, ExternalSubtree, sourceRoot, files,
	)
	if err != nil || result.Added != 1 || result.Files != 1 {
		t.Fatalf("external mirror: %#v %v", result, err)
	}
	target := filepath.Join(service.Root, course.Name, "external", "past-papers", "June.pdf")
	if content, readErr := os.ReadFile(target); readErr != nil || string(content) != string(body) {
		t.Fatalf("external file: %q %v", content, readErr)
	}
	if _, err = os.Stat(filepath.Join(service.Root, course.Name, "external", manifestName)); err != nil {
		t.Fatalf("external manifest: %v", err)
	}
	result, err = service.Sync(context.Background(), course, ExternalSubtree, sourceRoot, nil)
	if err != nil || result.Removed != 1 {
		t.Fatalf("external removal: %#v %v", result, err)
	}
	if _, err = os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("external stale file remains: %v", err)
	}
}

func pointer(ref blob.Reference) *blob.Reference {
	return &ref
}

func TestSyncRefusesUnownedAndEscapingDestinations(t *testing.T) {
	root := t.TempDir()
	courseDir := filepath.Join(root, "Networks")
	if err := os.MkdirAll(filepath.Join(courseDir, "eclass"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(courseDir, "eclass", "notes.txt"), []byte("user"), 0600); err != nil {
		t.Fatal(err)
	}
	service := Service{Root: root, Objects: fakeReader{}}
	file := File{Path: "/Courses/178/eclass/notes.txt", Object: pointer(fakeReference([]byte("new")))}
	if _, err := service.Sync(
		context.Background(), Course{ID: 178, Name: "Networks"}, EClassSubtree,
		"/Courses/178/eclass", []File{file},
	); err == nil {
		t.Fatal("unowned destination accepted")
	}
	escaping := File{Path: "/Courses/178/eclass/../outside.txt", Object: pointer(fakeReference([]byte("new")))}
	if _, err := service.Sync(
		context.Background(), Course{ID: 178, Name: "Networks"}, EClassSubtree,
		"/Courses/178/eclass", []File{escaping},
	); err == nil {
		t.Fatal("escaping destination accepted")
	}
}
