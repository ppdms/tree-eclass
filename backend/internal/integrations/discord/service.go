package discord

import (
	"context"
	"errors"
	"os"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
)

type Service struct {
	Pool    database.Store
	Objects *blob.Store
	Runner  Runner
	Temp    string
}

func checkSettings(ctx context.Context, tx database.Tx, cfg settings.Discord) error {
	return tx.Settings().VerifyDiscordExport(ctx, database.DiscordExportExpectation{
		Token:    identity.Encode(cfg.Token),
		Interval: cfg.Interval,
		Threads:  cfg.Threads,
		Media:    cfg.Media,
		Parallel: cfg.Parallel,
	})
}

func (s Service) Tick(ctx context.Context, now time.Time, forceDiscovery bool) error {
	cfg, err := (settings.Service{Pool: s.Pool}).Discord(ctx)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return nil
	}
	if cfg.Token == "" {
		return errors.New("Discord export is enabled without a token")
	}
	dir, err := os.MkdirTemp(s.Temp, "discord-export-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err = checkDirectory(dir); err != nil {
		return err
	}
	discover, err := s.Pool.Discord().DiscoveryDue(ctx)
	if err != nil {
		return err
	}
	if discover || forceDiscovery {
		if err = s.discover(ctx, dir, cfg); err != nil {
			return err
		}
	}
	source, err := s.next(ctx, now)
	if errors.Is(err, database.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = s.export(ctx, dir, cfg, source, now); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A failed interval retains its old cursor. Never log exporter output or token.
		saved := s.Pool.Discord().RecordExportFailure(ctx, database.DiscordExportFailure{
			Root:    source.Root,
			Channel: source.Channel,
			After:   source.After,
			NextAt:  now.Add(5 * time.Minute),
			Error:   "Discord interval failed; retry scheduled",
		})
		return errors.Join(err, saved)
	}
	return nil
}
func (s Service) next(ctx context.Context, now time.Time) (messages.Archive, error) {
	var source messages.Archive
	found, err := s.Pool.Discord().NextExportSource(ctx, now)
	if err != nil {
		return source, err
	}
	source.Root, source.Channel, source.Course, source.After = found.Root, found.Channel, found.Course, found.After
	return source, nil
}
func boundary(source messages.Archive, now time.Time) int64 {
	// Backfill at most one month per turn. Idle/old channels cannot produce an
	// unbounded initial export, and channels take turns during their backfill.
	start := source.After
	if start < source.Channel {
		start = source.Channel
	}
	millis := (start >> 22) + 1420070400000
	until := min(now.Add(-time.Minute).UnixMilli(), time.UnixMilli(millis).AddDate(0, 1, 0).UnixMilli())
	if until <= 1420070400000 {
		return 0
	}
	return (until - 1420070400000) << 22
}
