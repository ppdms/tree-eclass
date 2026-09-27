package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"tree-eclass/internal/domain/platform"
)

type providerSettings struct {
	Format int               `json:"format"`
	Keys   map[string]string `json:"keys"`
}

func (c *Controller) providerPath() string {
	return filepath.Join(c.active(), "settings", "provider-keys.json")
}

func (c *Controller) providerKeys() (map[string]string, error) {
	path := c.providerPath()
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1024*1024 {
		return nil, errors.New("provider settings must be a private regular file under 1 MiB")
	}
	var value providerSettings
	if err = platform.ReadJSON(path, &value); err != nil {
		return nil, errors.New("invalid checkpointed provider settings")
	}
	if value.Format != 1 || value.Keys == nil {
		return nil, errors.New("unsupported provider settings format")
	}
	if err = validateProviderKeys(value.Keys); err != nil {
		return nil, err
	}
	return value.Keys, nil
}

func validateProviderKeys(keys map[string]string) error {
	for name, value := range keys {
		if !slices.Contains(providerSecrets, name) {
			return errors.New("provider settings contain an unsupported key")
		}
		if len(value) > 64*1024 {
			return fmt.Errorf("provider key %s exceeds 64 KiB", name)
		}
	}
	data, err := json.Marshal(providerSettings{1, keys})
	if err != nil {
		return err
	}
	if len(data) > 512*1024 {
		return errors.New("provider settings exceed 512 KiB")
	}
	return nil
}

func (c *Controller) ensureProviderKeys(ctx context.Context) error {
	return c.ensureProviderKeysFrom(ctx, c.legacyProviderKeys)
}

func (c *Controller) ensureProviderKeysFrom(
	ctx context.Context,
	load func(context.Context) (map[string]string, error),
) error {
	if _, err := c.providerKeys(); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if c.State.Baseline != "" {
		return errors.New(
			"provider settings are missing from the development baseline; run tree down before importing credentials",
		)
	}
	return c.importProviderKeysFrom(ctx, load)
}

func (c *Controller) ImportProviderKeys(ctx context.Context) error {
	if err := c.configured(); err != nil {
		return err
	}
	return c.importProviderKeysFrom(ctx, c.legacyProviderKeys)
}

func (c *Controller) importProviderKeysFrom(
	ctx context.Context,
	load func(context.Context) (map[string]string, error),
) error {
	if c.State.Baseline != "" {
		return errors.New("exit development with tree down before importing stable provider credentials")
	}
	if err := c.stopped(); err != nil {
		return fmt.Errorf("stop the native runtime before importing provider credentials: %w", err)
	}
	keys, err := load(ctx)
	if err != nil {
		return err
	}
	if keys == nil {
		keys = map[string]string{}
	}
	if err = validateProviderKeys(keys); err != nil {
		return err
	}
	return platform.WriteJSON(c.providerPath(), providerSettings{1, keys})
}
