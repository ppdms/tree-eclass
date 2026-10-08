package messages

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"time"

	"tree-eclass/internal/domain/database"
)

// ImportInterval commits every partition and the exclusive cursor together.
// The callback binds the originating settings after helper I/O has completed.
func (s Importer) ImportInterval(
	ctx context.Context,
	source Archive,
	parts []string,
	next time.Time,
	validate func(database.Tx) error,
) error {
	if source.Root <= 0 || source.Course <= 0 || source.Channel <= 0 || source.Before <= source.After ||
		len(parts) > 64 {
		return errors.New("invalid Discord interval")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = validate(tx); err != nil {
		return err
	}
	if err = lockArchive(ctx, tx, source, ""); err != nil {
		return err
	}
	if err = lockCursor(ctx, tx, source); err != nil {
		return err
	}
	if err = s.importPartitions(ctx, tx, source, parts); err != nil {
		return err
	}
	if err = tx.DiscordImports().AdvanceExportCursor(ctx, database.DiscordExportCursor{
		RootID:    source.Root,
		ChannelID: source.Channel,
		AfterID:   source.Before - 1,
		NextAt:    next,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func lockCursor(ctx context.Context, tx database.Tx, source Archive) error {
	after, err := tx.DiscordImports().ExportCursorForUpdate(ctx, source.Root, source.Channel)
	if database.IsNoRows(err) {
		after, err = 0, nil
	}
	if err != nil {
		return err
	}
	if after != source.After {
		return errors.New("Discord export cursor changed before publication")
	}
	return nil
}

func (s Importer) importPartitions(ctx context.Context, tx database.Tx, source Archive, parts []string) error {
	var total int64
	published := []string{}
	for _, path := range parts {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("Discord partition is not a regular file")
		}
		total += info.Size()
		if total > 256*1024*1024 {
			return errors.New("Discord interval exceeds 256 MiB")
		}
		published, err = s.importPartition(ctx, tx, source, path, published)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s Importer) importPartition(
	ctx context.Context,
	tx database.Tx,
	source Archive,
	path string,
	published []string,
) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return published, err
	}
	result, importErr := s.importTx(ctx, tx, source, io.Reader(file))
	closeErr := file.Close()
	if err = errors.Join(importErr, closeErr); err != nil {
		return published, err
	}
	if slices.Contains(published, result.Path) {
		return published, errors.New("duplicate Discord partition")
	}
	overlap, err := tx.DiscordImports().StagedOverlapsPublished(ctx, published)
	if err != nil {
		return published, err
	}
	if overlap {
		return published, errors.New("overlapping Discord export partitions")
	}
	return append(published, result.Path), nil
}
