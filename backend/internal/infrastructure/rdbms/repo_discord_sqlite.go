package rdbms

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"tree-eclass/internal/domain/database"
)

type sqliteDiscord struct{ db nativeDBTX }

func sqliteTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000000000")
}

func (d sqliteDiscord) DiscoveryDue(ctx context.Context) (bool, error) {
	var due bool
	err := d.db.QueryRow(ctx, `SELECT NOT EXISTS(
		SELECT 1 FROM message_state
		WHERE key='native_discovery'
		AND datetime(updated_at)>datetime('now','-6 hours'))`).Scan(&due)
	return due, err
}

func (d sqliteDiscord) ReplaceDiscovery(ctx context.Context, channels []database.DiscoveredChannel) error {
	if _, err := d.db.Exec(ctx, `DELETE FROM discord_discovered_channels`); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `DELETE FROM discord_root_channels`); err != nil {
		return err
	}
	for _, ch := range channels {
		thread := 0
		if ch.IsThread {
			thread = 1
		}
		if _, err := d.db.Exec(ctx, `INSERT INTO discord_discovered_channels(channel_id,root_id,guild_id,name,is_thread)
			VALUES(?,?,?,?,?)`, ch.ChannelID, ch.RootID, ch.GuildID, ch.Name, thread); err != nil {
			return err
		}
		if ch.IsThread {
			continue
		}
		metadata, err := json.Marshal(map[string]string{"guild_id": fmt.Sprint(ch.GuildID)})
		if err != nil {
			return err
		}
		if _, err := d.db.Exec(ctx, `INSERT INTO discord_root_channels(root_channel_id,name,metadata)
			VALUES(?,?,?)`, fmt.Sprint(ch.ChannelID), ch.Name, string(metadata)); err != nil {
			return err
		}
	}
	_, err := d.db.Exec(ctx, `INSERT INTO message_state(key,value,updated_at)
		VALUES('native_discovery','ready',strftime('%Y-%m-%d %H:%M:%S','now'))
		ON CONFLICT(key) DO UPDATE SET value='ready',updated_at=excluded.updated_at`)
	return err
}

func (d sqliteDiscord) NextExportSource(ctx context.Context, now time.Time) (database.ExportSource, error) {
	var source database.ExportSource
	err := d.db.QueryRow(ctx, `SELECT d.root_id,d.channel_id,m.course_id,coalesce(c.after_id,0)
		FROM discord_discovered_channels d
		JOIN discord_course_channels m ON m.root_channel_id=CAST(d.root_id AS TEXT)
		LEFT JOIN export_cursors c ON c.root_id=d.root_id AND c.channel_id=d.channel_id
		WHERE c.next_at IS NULL OR c.next_at<=?
		ORDER BY c.next_at IS NOT NULL,c.next_at,d.channel_id LIMIT 1`, sqliteTimestamp(now)).
		Scan(&source.Root, &source.Channel, &source.Course, &source.After)
	return source, err
}

func (d sqliteDiscord) RecordExportFailure(ctx context.Context, failure database.DiscordExportFailure) error {
	_, err := d.db.Exec(ctx, `INSERT INTO export_cursors(root_id,channel_id,after_id,next_at,error)
		VALUES(?,?,?,?,?)
		ON CONFLICT(root_id,channel_id) DO UPDATE SET next_at=excluded.next_at,error=excluded.error`,
		failure.Root, failure.Channel, failure.After, sqliteTimestamp(failure.NextAt), failure.Error)
	return err
}
