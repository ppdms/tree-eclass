package blob

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"

	"tree-eclass/internal/domain/platform"
)

// Download verifies immutable source identity while streaming to an owned spool.
// The caller removes the returned file after its last consumer finishes.
func (s *Store) Download(ctx context.Context, ref Reference, temp string) (string, error) {
	if ref.Bytes < 0 || ref.Bytes > MaxSourceBytes {
		return "", errors.New("source exceeds download limits")
	}
	free, err := platform.Available(temp)
	if err != nil {
		return "", err
	}
	if free < 5*1024*1024*1024+uint64(ref.Bytes) {
		return "", errors.New("source download paused: insufficient space for source plus 5 GiB reserve")
	}
	out, err := s.Get(ctx, ref, "")
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	f, err := os.CreateTemp(temp, "source-*")
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(out.Body, MaxSourceBytes+1))
	if err == nil && (n != ref.Bytes || fmt.Sprintf("%x", hash.Sum(nil)) != ref.SHA256) {
		err = errors.New("stored source content does not match its revision")
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}
