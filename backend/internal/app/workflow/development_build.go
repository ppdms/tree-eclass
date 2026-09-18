package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

var errSourceChanged = errors.New("source changed during compilation; rebuilding the newer revision")

func (c *Controller) buildDevelopment(ctx context.Context, target string) (string, error) {
	if err := c.space(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return "", err
	}
	source, err := os.MkdirTemp(filepath.Dir(target), "source-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(source)
	digest, err := sourceDigest(c.Repo, source)
	if err != nil {
		return "", err
	}
	if err = c.run(ctx, source, "go", "-C", "backend", "build", "-trimpath", "-o", target, "./cmd/tree-eclass"); err != nil {
		return "", err
	}
	current, err := sourceDigest(c.Repo, "")
	if err != nil {
		return "", err
	}
	if current != digest {
		return "", errSourceChanged
	}
	return digest, nil
}
