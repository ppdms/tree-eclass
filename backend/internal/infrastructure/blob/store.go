// Package blob implements immutable, content-addressed document storage on the
// local filesystem. Every object is the file <root>/<sha256>; catalog identity
// records bucket tree-eclass-data, key objects/<sha256> and the SHA-256 itself
// as the version ID.
package blob

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/objects"
)

// DataBucket is the catalog bucket recorded for every stored object. It is
// historical identity only: bytes live under the objects root directory.
const DataBucket = "tree-eclass-data"

// Reference and MaxSourceBytes are the shared domain contract from
// domain/objects; they remain identical types for every caller.
type Reference = objects.Reference

const MaxSourceBytes = objects.MaxSourceBytes

// Store keeps immutable documents as content-addressed files under one root.
type Store struct {
	root string
}

// New returns a store rooted at the objects directory. Setup must still run
// before first use.
func New(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("objects root is required")
	}
	return &Store{root: root}, nil
}

func (s *Store) path(digest string) string { return filepath.Join(s.root, digest) }

// Setup creates the objects directory with private permissions. It is
// idempotent and never inspects or repairs existing content.
func (s *Store) Setup(ctx context.Context) error {
	return os.MkdirAll(s.root, 0o700)
}

// Put spools to a bounded temporary file, computes a real SHA-256, and stores
// the spooled bytes under that digest. No document-sized allocation. Storing
// content that already exists verifies the stored file and returns its
// reference unchanged.
func (s *Store) Put(ctx context.Context, input io.Reader, mediaType, tempDir string) (Reference, error) {
	f, err := os.CreateTemp(tempDir, "upload-*")
	if err != nil {
		return Reference{}, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(input, MaxSourceBytes+1))
	if err != nil {
		return Reference{}, err
	}
	if size == 0 || size > MaxSourceBytes {
		return Reference{}, errors.New("document must contain between 1 byte and 50 MiB")
	}
	digest := fmt.Sprintf("%x", hash.Sum(nil))
	ref := Reference{
		Bucket: DataBucket, Key: "objects/" + digest, VersionID: digest,
		SHA256: digest, Bytes: size, MediaType: mediaType,
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return ref, err
	}
	created, err := os.OpenFile(s.path(digest), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return ref, s.verify(ref)
	}
	if err != nil {
		return ref, err
	}
	if _, err = io.Copy(created, f); err == nil {
		err = created.Sync()
	}
	if closeErr := created.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(s.path(digest))
		return ref, err
	}
	return ref, nil
}

// verify re-hashes an already stored object so an existing file is returned
// only when its size and digest match the newly spooled bytes.
func (s *Store) verify(ref Reference) error {
	f, err := os.Open(s.path(ref.SHA256))
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, f)
	if err == nil && (size != ref.Bytes || fmt.Sprintf("%x", hash.Sum(nil)) != ref.SHA256) {
		err = errors.New("uploaded object verification failed")
	}
	return err
}

// Open streams the stored object. VersionID is ignored: files are
// content-addressed and the digest was verified when they were written.
func (s *Store) Open(ctx context.Context, ref Reference) (io.ReadCloser, error) {
	return os.Open(s.path(ref.SHA256))
}
