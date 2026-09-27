package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store, root
}

type endless struct{}

func (endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestPutStoresContentAddressedFiles(t *testing.T) {
	store, root := newTestStore(t)
	ctx := t.Context()
	content := []byte("phrase repeated for digest stability")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	first, err := store.Put(ctx, bytes.NewReader(content), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := Reference{Bucket: DataBucket, Key: "objects/" + digest, VersionID: digest,
		SHA256: digest, Bytes: int64(len(content)), MediaType: "text/plain"}
	if first != want {
		t.Fatalf("reference mismatch: %+v vs %+v", first, want)
	}
	stored, err := os.ReadFile(filepath.Join(root, digest))
	if err != nil || !bytes.Equal(stored, content) {
		t.Fatal("stored file differs from source", err)
	}
	second, err := store.Put(ctx, bytes.NewReader(content), "text/plain", t.TempDir())
	if err != nil || second != want {
		t.Fatal("idempotent put failed", second, err)
	}
}

func TestPutVerifiesExistingFileBeforeReturningIt(t *testing.T) {
	store, root := newTestStore(t)
	content := []byte("original")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	if err := os.WriteFile(filepath.Join(root, digest), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := store.Put(t.Context(), bytes.NewReader(content), "text/plain", t.TempDir())
	if err == nil || err.Error() != "uploaded object verification failed" {
		t.Fatal("existing mismatched file accepted", err)
	}
}

func TestPutRejectsEmptyAndOversizeDocuments(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	_, err := store.Put(ctx, strings.NewReader(""), "text/plain", t.TempDir())
	if err == nil || err.Error() != "document must contain between 1 byte and 50 MiB" {
		t.Fatal("empty document accepted", err)
	}
	_, err = store.Put(ctx, io.LimitReader(endless{}, MaxSourceBytes+1), "application/octet-stream", t.TempDir())
	if err == nil || err.Error() != "document must contain between 1 byte and 50 MiB" {
		t.Fatal("oversize document accepted", err)
	}
}

func TestOpenReportsMissingObjects(t *testing.T) {
	store, _ := newTestStore(t)
	missing := Reference{Bucket: DataBucket, Key: "objects/" + strings.Repeat("a", 64), SHA256: strings.Repeat("a", 64)}
	if _, err := store.Open(t.Context(), missing); err == nil {
		t.Fatal("missing object opened")
	}
}

func TestDownloadVerifiesSourceIdentity(t *testing.T) {
	store, root := newTestStore(t)
	ctx := t.Context()
	content := []byte("download me")
	ref, err := store.Put(ctx, bytes.NewReader(content), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.Download(ctx, ref, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stored, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(stored, content) {
		t.Fatal("downloaded content differs from source", readErr)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, ref.SHA256), bytes.Repeat([]byte("x"), len(content)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = store.Download(ctx, ref, t.TempDir())
	if err == nil || err.Error() != "stored source content does not match its revision" {
		t.Fatal("altered source accepted", err)
	}
	absent := ref
	absent.SHA256 = strings.Repeat("0", 64)
	if _, err = store.Download(ctx, absent, t.TempDir()); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestDownloadRejectsImpossibleSources(t *testing.T) {
	store, _ := newTestStore(t)
	oversize := Reference{SHA256: strings.Repeat("a", 64), Bytes: MaxSourceBytes + 1}
	_, err := store.Download(t.Context(), oversize, t.TempDir())
	if err == nil || err.Error() != "source exceeds download limits" {
		t.Fatal("oversize source accepted", err)
	}
	negative := oversize
	negative.Bytes = -1
	_, err = store.Download(t.Context(), negative, t.TempDir())
	if err == nil || err.Error() != "source exceeds download limits" {
		t.Fatal("negative source accepted", err)
	}
}

func TestPruneRemovesOnlyUnregisteredObjects(t *testing.T) {
	store, root := newTestStore(t)
	ctx := t.Context()
	keepRef, err := store.Put(ctx, strings.NewReader("registered"), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dropRef, err := store.Put(ctx, strings.NewReader("abandoned"), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	foreign := []string{"notes.txt", strings.Repeat("z", 64), strings.ToUpper(keepRef.SHA256)}
	for _, name := range foreign {
		if err = os.WriteFile(filepath.Join(root, name), []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.MkdirAll(filepath.Join(root, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "subdir", strings.Repeat("9", 64))
	if err = os.WriteFile(nested, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := store.PruneUnregistered(ctx, func(_ context.Context, keys, versions []string) ([]bool, error) {
		if len(keys) != 2 {
			t.Fatalf("unexpected candidate set %v %v", keys, versions)
		}
		keep := make([]bool, len(keys))
		for i, key := range keys {
			if versions[i] != keepRef.SHA256 && versions[i] != dropRef.SHA256 {
				t.Fatal("unexpected version", versions[i])
			}
			keep[i] = key == keepRef.Key
		}
		return keep, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Versions != 1 || result.Bytes != int64(len("abandoned")) {
		t.Fatal("sweep accounting wrong", result)
	}
	if _, err = os.Stat(filepath.Join(root, keepRef.SHA256)); err != nil {
		t.Fatal("registered file removed", err)
	}
	if _, err = os.Stat(filepath.Join(root, dropRef.SHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unregistered file kept", err)
	}
	for _, name := range foreign {
		if _, err = os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatal("foreign file removed", name, err)
		}
	}
	if _, err = os.Stat(nested); err != nil {
		t.Fatal("subdirectory contents removed", err)
	}
}

func TestPrunePropagatesRegistrationFailures(t *testing.T) {
	store, root := newTestStore(t)
	ctx := t.Context()
	ref, err := store.Put(ctx, strings.NewReader("kept until verified"), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PruneUnregistered(ctx, func(context.Context, []string, []string) ([]bool, error) {
		return nil, errors.New("database unavailable")
	})
	if err == nil || err.Error() != "database unavailable" {
		t.Fatal("registration error swallowed", err)
	}
	_, err = store.PruneUnregistered(ctx, func(context.Context, []string, []string) ([]bool, error) {
		return nil, nil
	})
	if err == nil || err.Error() != "incomplete object reference verification" {
		t.Fatal("incomplete verification accepted", err)
	}
	if _, err = os.Stat(filepath.Join(root, ref.SHA256)); err != nil {
		t.Fatal("file removed despite failure", err)
	}
}

func TestSetupAndCheckValidateObjectsRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "nested", "objects")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = New(""); err == nil {
		t.Fatal("empty root accepted")
	}
	if err = store.Check(t.Context()); err == nil {
		t.Fatal("check passed before setup")
	}
	if err = store.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = store.Setup(t.Context()); err != nil {
		t.Fatal("setup is not idempotent", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		t.Fatal("root missing after setup", err)
	}
	if err = store.Check(t.Context()); err != nil {
		t.Fatal("check failed on ready root", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("probe file left behind", entries, err)
	}
	if err = os.WriteFile(filepath.Join(base, "plain"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	fileStore, err := New(filepath.Join(base, "plain"))
	if err != nil {
		t.Fatal(err)
	}
	if err = fileStore.Check(t.Context()); err == nil {
		t.Fatal("check accepted a plain file as root")
	}
}
