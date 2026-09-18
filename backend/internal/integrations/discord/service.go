package discord

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
)

type Service struct {
	Pool    *pgxpool.Pool
	Objects *blob.Store
	Runner  Runner
	Temp    string
}

func checkSettings(ctx context.Context, tx pgx.Tx, cfg settings.Discord) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('settings:discord-export',0))`); err != nil {
		return err
	}
	var matches bool
	err := tx.QueryRow(ctx, `SELECT enabled=1 AND token=$1 AND interval_seconds=$2 AND include_threads=$3 AND media=$4 AND parallel=$5 FROM app.discord_export_settings WHERE id=1`, identity.Encode(cfg.Token), cfg.Interval, cfg.Threads, boolInt(cfg.Media), cfg.Parallel).
		Scan(&matches)
	if err != nil {
		return err
	}
	if !matches {
		return errors.New("Discord settings changed during export")
	}
	return nil
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
	var discover bool
	err = s.Pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM messages.message_state WHERE key='native_discovery' AND updated_at::timestamptz>clock_timestamp()-interval '6 hours')`).
		Scan(&discover)
	if err != nil {
		return err
	}
	if discover || forceDiscovery {
		if err = s.discover(ctx, dir, cfg); err != nil {
			return err
		}
	}
	source, err := s.next(ctx, now)
	if errors.Is(err, pgx.ErrNoRows) {
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
		_, saved := s.Pool.Exec(
			ctx,
			`INSERT INTO messages.export_cursors(root_id,channel_id,after_id,next_at,error) VALUES($1,$2,$3,$4,$5) ON CONFLICT(root_id,channel_id) DO UPDATE SET next_at=excluded.next_at,error=excluded.error`,
			source.Root,
			source.Channel,
			source.After,
			now.Add(5*time.Minute),
			"Discord interval failed; retry scheduled",
		)
		return errors.Join(err, saved)
	}
	return nil
}
func (s Service) next(ctx context.Context, now time.Time) (messages.Archive, error) {
	var source messages.Archive
	err := s.Pool.QueryRow(ctx, `SELECT d.root_id,d.channel_id,m.course_id,coalesce(c.after_id,0) FROM app.discord_discovered_channels d
 JOIN app.discord_course_channels m ON m.root_channel_id=d.root_id::text
 LEFT JOIN messages.export_cursors c ON c.root_id=d.root_id AND c.channel_id=d.channel_id
 WHERE c.next_at IS NULL OR c.next_at<=$1 ORDER BY coalesce(c.next_at,'epoch'::timestamptz),d.channel_id LIMIT 1`, now).
		Scan(&source.Root, &source.Channel, &source.Course, &source.After)
	return source, err
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
