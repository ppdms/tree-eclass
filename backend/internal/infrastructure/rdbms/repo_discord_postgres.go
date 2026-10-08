package rdbms

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"tree-eclass/internal/domain/database"
)

type postgresDiscord struct{ db nativeDBTX }

func (d postgresDiscord) DiscoveryDue(ctx context.Context) (bool, error) {
	var due bool
	err := d.db.QueryRow(ctx, `SELECT NOT EXISTS(
		SELECT 1 FROM messages.message_state
		WHERE key='native_discovery'
		AND updated_at::timestamptz>clock_timestamp()-interval '6 hours')`).Scan(&due)
	return due, err
}

func (d postgresDiscord) ReplaceDiscovery(ctx context.Context, channels []database.DiscoveredChannel) error {
	if _, err := d.db.Exec(ctx, `DELETE FROM app.discord_discovered_channels`); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `DELETE FROM app.discord_root_channels`); err != nil {
		return err
	}
	for _, ch := range channels {
		if _, err := d.db.Exec(ctx, `INSERT INTO app.discord_discovered_channels(channel_id,root_id,guild_id,name,is_thread)
			VALUES($1,$2,$3,$4,$5)`, ch.ChannelID, ch.RootID, ch.GuildID, ch.Name, ch.IsThread); err != nil {
			return err
		}
		if ch.IsThread {
			continue
		}
		metadata, err := json.Marshal(map[string]string{"guild_id": fmt.Sprint(ch.GuildID)})
		if err != nil {
			return err
		}
		if _, err := d.db.Exec(ctx, `INSERT INTO app.discord_root_channels(root_channel_id,name,metadata)
			VALUES($1,$2,$3)`, fmt.Sprint(ch.ChannelID), ch.Name, metadata); err != nil {
			return err
		}
	}
	_, err := d.db.Exec(ctx, `INSERT INTO messages.message_state(key,value,updated_at)
		VALUES('native_discovery','ready',clock_timestamp()::text)
		ON CONFLICT(key) DO UPDATE SET value='ready',updated_at=excluded.updated_at`)
	return err
}

func (d postgresDiscord) NextExportSource(ctx context.Context, now time.Time) (database.ExportSource, error) {
	var source database.ExportSource
	err := d.db.QueryRow(ctx, `SELECT d.root_id,d.channel_id,m.course_id,coalesce(c.after_id,0)
		FROM app.discord_discovered_channels d
		JOIN app.discord_course_channels m ON m.root_channel_id=d.root_id::text
		LEFT JOIN messages.export_cursors c ON c.root_id=d.root_id AND c.channel_id=d.channel_id
		WHERE c.next_at IS NULL OR c.next_at<=$1
		ORDER BY coalesce(c.next_at,'epoch'::timestamptz),d.channel_id LIMIT 1`, now).
		Scan(&source.Root, &source.Channel, &source.Course, &source.After)
	return source, err
}

func (d postgresDiscord) RecordExportFailure(ctx context.Context, failure database.DiscordExportFailure) error {
	_, err := d.db.Exec(ctx, `INSERT INTO messages.export_cursors(root_id,channel_id,after_id,next_at,error)
		VALUES($1,$2,$3,$4,$5)
		ON CONFLICT(root_id,channel_id) DO UPDATE SET next_at=excluded.next_at,error=excluded.error`,
		failure.Root, failure.Channel, failure.After, failure.NextAt, failure.Error)
	return err
}
