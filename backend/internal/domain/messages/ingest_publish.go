package messages

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
)

func lockArchive(ctx context.Context, tx pgx.Tx, s Archive, path string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('settings:discord-map',0))`); err != nil {
		return err
	}
	var mapped int64
	if err := tx.QueryRow(ctx, `SELECT course_id FROM app.discord_course_channels WHERE root_channel_id=$1`, fmt.Sprint(s.Root)).Scan(&mapped); err != nil {
		return err
	}
	if mapped != s.Course {
		return errors.New("Discord channel mapping changed during import")
	}
	courses := []int64{s.Course}
	var previous int64
	err := tx.QueryRow(ctx, `SELECT course_id FROM messages.archive_sources WHERE path=$1`, path).Scan(&previous)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && previous != s.Course {
		courses = append(courses, previous)
	}
	slices.Sort(courses)
	for _, id := range courses {
		if err = queries.New(tx).QueueLock(ctx, fmt.Sprintf("course:%d", id)); err != nil {
			return err
		}
	}
	return nil
}
func publishArchive(ctx context.Context, tx pgx.Tx, s Archive, h exportHeader, result ImportResult) error {
	// Cascades discard only this artifact's derived rows, in the same transaction.
	if _, err := tx.Exec(ctx, `DELETE FROM messages.archive_sources WHERE path=$1`, result.Path); err != nil {
		return err
	}
	_, err := tx.Exec(
		ctx,
		`INSERT INTO messages.archive_sources(path,root_id,course_id,fingerprint,sha256,channel_id,exported_at,status,indexed_at,object_id)
 VALUES($1,$2,$3,$4,$4,$5,$6,'ready',clock_timestamp()::text,$4)`,
		result.Path,
		fmt.Sprint(s.Root),
		s.Course,
		result.Object.SHA256,
		s.Channel,
		h.Exported,
	)
	if err != nil {
		return err
	}
	var parent *int64
	if h.Channel.CategoryID > 0 {
		v := int64(h.Channel.CategoryID)
		parent = &v
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO messages.channels(channel_id,root_id,course_id,guild_id,channel_name,channel_type,parent_channel_id,topic,source_path,exported_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(channel_id) DO UPDATE SET root_id=excluded.root_id,course_id=excluded.course_id,guild_id=excluded.guild_id,channel_name=excluded.channel_name,channel_type=excluded.channel_type,parent_channel_id=excluded.parent_channel_id,topic=excluded.topic,source_path=excluded.source_path,exported_at=excluded.exported_at`,
		s.Channel,
		s.Root,
		s.Course,
		int64(h.Guild.ID),
		identity.Encode(h.Channel.Name),
		h.Channel.Type,
		parent,
		identity.Encode(h.Channel.Topic),
		result.Path,
		h.Exported,
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO messages.messages(message_id,channel_id,course_id,timestamp,timestamp_epoch,author_key,author_name,content,searchable_text,reply_to_message_id,message_type,is_pinned,reaction_count,attachment_metadata_json,source_path)
 SELECT message_id,$1,$2,timestamp,timestamp_epoch,author_key,author_name,content,searchable_text,reply_to_message_id,message_type,is_pinned,reaction_count,attachment_metadata_json,$3 FROM tree_discord_stage ORDER BY message_id`,
		s.Channel,
		s.Course,
		result.Path,
	)
	return err
}
