package discord

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
)

func (s Service) export(
	ctx context.Context,
	dir string,
	cfg settings.Discord,
	source messages.Archive,
	now time.Time,
) error {
	source.Before = boundary(source, now)
	if source.Before <= source.After+1 {
		return nil
	}
	args := []string{
		"export",
		"--channel",
		fmt.Sprint(source.Channel),
		"--format",
		"Json",
		"--output",
		filepath.Join(dir, "export.json"),
		"--before",
		fmt.Sprint(source.Before),
		"--include-threads",
		"None",
		"--parallel",
		"1",
		"--partition",
		"5mb",
		"--utc",
		"true",
		"--markdown",
		"false",
	}
	if source.After > 0 {
		args = append(args, "--after", fmt.Sprint(source.After))
	}
	if cfg.Media {
		args = append(args, "--media", "true", "--media-dir", filepath.Join(dir, "media"))
	}
	output, err := s.Runner.Run(ctx, dir, cfg.Token, args...)
	if err != nil {
		return err
	}
	if !strings.Contains(output, "Successfully exported 1 channel(s).") ||
		strings.Contains(output, "Failed to export the following channel(s):") {
		return errors.New("Discord exporter did not confirm a complete single-channel interval")
	}
	if err = checkDirectory(dir); err != nil {
		return err
	}
	parts, media, err := s.artifacts(ctx, dir)
	if err != nil {
		return err
	}
	source.Media = media
	next := now.Add(time.Duration(cfg.Interval) * time.Second)
	if source.Before < ((now.Add(-time.Minute).UnixMilli() - 1420070400000) << 22) {
		next = now
	}
	importer := messages.Importer{Pool: s.Pool, Blobs: s.Objects, Temp: s.Temp}
	return importer.ImportInterval(
		ctx,
		source,
		parts,
		next,
		func(tx rdbms.Tx) error { return checkSettings(ctx, tx, cfg) },
	)
}
func (s Service) artifacts(ctx context.Context, dir string) ([]string, map[string]blob.Reference, error) {
	parts := []string{}
	media := map[string]blob.Reference{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("nonregular Discord artifact")
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if strings.HasPrefix(relative, "media/") {
			object, putErr := s.putMedia(ctx, path)
			if putErr != nil {
				return putErr
			}
			media[relative] = object
			return nil
		}
		if filepath.Base(relative) == relative && strings.HasPrefix(relative, "export") &&
			strings.HasSuffix(relative, ".json") {
			parts = append(parts, path)
			return nil
		}
		return errors.New("unexpected Discord export artifact")
	})
	sort.Strings(parts)
	if len(parts) > 64 {
		return nil, nil, errors.New("Discord export exceeds 64 partitions")
	}
	return parts, media, err
}

func (s Service) putMedia(ctx context.Context, path string) (blob.Reference, error) {
	input, err := os.Open(path)
	if err != nil {
		return blob.Reference{}, err
	}
	head := make([]byte, 512)
	n, _ := input.Read(head)
	if _, err = input.Seek(0, 0); err != nil {
		input.Close()
		return blob.Reference{}, err
	}
	object, err := s.Objects.Put(ctx, input, http.DetectContentType(head[:n]), s.Temp)
	closeErr := input.Close()
	if err != nil {
		return blob.Reference{}, err
	}
	if closeErr != nil {
		return blob.Reference{}, closeErr
	}
	return object, nil
}
