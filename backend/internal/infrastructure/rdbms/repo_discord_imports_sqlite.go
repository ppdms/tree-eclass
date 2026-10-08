package rdbms

import (
	"context"
	"fmt"
	"strings"

	"tree-eclass/internal/domain/database"
)

// sqliteDiscordImports implements the Discord archive staging/publication
// port with native SQLite SQL. The staging area is a TEMP TABLE scoped to
// the caller's transaction connection; writer admission serializes staging
// against concurrent writers. No PostgreSQL syntax is used anywhere.
type sqliteDiscordImports struct{ db nativeDBTX }

func (d sqliteDiscordImports) BeginStage(ctx context.Context) error {
	if _, err := d.db.Exec(ctx, `DROP TABLE IF EXISTS tree_discord_stage`); err != nil {
		return err
	}
	_, err := d.db.Exec(ctx, `CREATE TEMP TABLE tree_discord_stage(
		message_id INTEGER PRIMARY KEY,
		timestamp TEXT NOT NULL,
		timestamp_epoch REAL NOT NULL,
		author_key TEXT,
		author_name TEXT NOT NULL,
		content TEXT NOT NULL,
		searchable_text TEXT NOT NULL,
		reply_to_message_id INTEGER,
		message_type TEXT NOT NULL,
		is_pinned INTEGER NOT NULL,
		reaction_count INTEGER NOT NULL,
		attachment_metadata_json TEXT NOT NULL)`)
	return err
}

func (d sqliteDiscordImports) AppendStagedMessages(
	ctx context.Context, messages []database.DiscordStagedMessage,
) error {
	const chunk = 500
	const cols = `message_id,timestamp,timestamp_epoch,author_key,author_name,` +
		`content,searchable_text,reply_to_message_id,message_type,` +
		`is_pinned,reaction_count,attachment_metadata_json`
	for start := 0; start < len(messages); start += chunk {
		end := start + chunk
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[start:end]
		var sb strings.Builder
		sb.WriteString(`INSERT INTO tree_discord_stage(` + cols + `) VALUES `)
		args := make([]any, 0, len(batch)*12)
		for i, m := range batch {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?)")
			args = append(args,
				m.ID, m.Timestamp, m.Epoch, m.AuthorKey, m.AuthorName,
				m.Content, m.SearchText, m.ReplyTo, m.MessageType,
				m.Pinned, m.Reactions, m.Attachments)
		}
		if _, err := d.db.Exec(ctx, sb.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

func (d sqliteDiscordImports) StagedMessages(
	ctx context.Context, afterID int64, limit int,
) (database.Iterator[database.DiscordStagedMessage], error) {
	rows, err := d.db.Query(ctx, `SELECT message_id,timestamp,timestamp_epoch,
		author_key,author_name,content,searchable_text,reply_to_message_id,
		message_type,is_pinned,reaction_count,attachment_metadata_json
		FROM tree_discord_stage WHERE message_id>? ORDER BY message_id LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanDiscordStaged), nil
}

func (d sqliteDiscordImports) ReplyContext(ctx context.Context, messageID, courseID, guildID int64) (string, error) {
	var content string
	err := d.db.QueryRow(ctx, `SELECT content FROM (
		SELECT content,0 priority,'' indexed_at
			FROM tree_discord_stage WHERE message_id=?
		UNION ALL
		SELECT m.content,1,a.indexed_at
		FROM messages m
		JOIN archive_sources a
			ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
		JOIN discord_course_channels mapping
			ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
		JOIN channels ch
			ON ch.channel_id=m.channel_id AND ch.course_id=m.course_id AND ch.guild_id=?
		WHERE m.message_id=? AND m.course_id=?) candidates
		ORDER BY priority,indexed_at DESC LIMIT 1`,
		messageID, guildID, messageID, courseID).Scan(&content)
	return content, err
}

func (d sqliteDiscordImports) StagedOverlapsPublished(ctx context.Context, paths []string) (bool, error) {
	if len(paths) == 0 {
		return false, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(paths)), ",")
	args := make([]any, len(paths))
	for i, path := range paths {
		args[i] = path
	}
	var overlap bool
	err := d.db.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM messages m
		JOIN tree_discord_stage staged ON staged.message_id=m.message_id
		WHERE m.source_path IN (`+placeholders+`))`, args...).Scan(&overlap)
	return overlap, err
}

func (d sqliteDiscordImports) LockDiscordMapping(ctx context.Context) error {
	_, err := advisoryLock(ctx, d.db, "settings:discord-map", true, false)
	return err
}

func (d sqliteDiscordImports) MappedCourseForRoot(ctx context.Context, rootID string) (int64, error) {
	var course int64
	err := d.db.QueryRow(ctx,
		`SELECT course_id FROM discord_course_channels WHERE root_channel_id=?`, rootID).Scan(&course)
	return course, err
}

func (d sqliteDiscordImports) ArchiveSourceCourse(ctx context.Context, path string) (int64, error) {
	var course int64
	err := d.db.QueryRow(ctx,
		`SELECT course_id FROM archive_sources WHERE path=?`, path).Scan(&course)
	return course, err
}

func (d sqliteDiscordImports) ArchiveReady(
	ctx context.Context, identity database.DiscordArchiveIdentity,
) (bool, error) {
	var ready bool
	err := d.db.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM archive_sources
		WHERE path=? AND course_id=? AND fingerprint=? AND object_id=? AND status='ready')`,
		identity.Path, identity.CourseID, identity.SHA256, identity.SHA256).Scan(&ready)
	return ready, err
}

func (d sqliteDiscordImports) ArchiveConversationCount(ctx context.Context, path string) (int64, error) {
	var count int64
	err := d.db.QueryRow(ctx,
		`SELECT count(*) FROM conversations WHERE source_path=?`, path).Scan(&count)
	return count, err
}

func (d sqliteDiscordImports) PublishArchive(ctx context.Context, p database.DiscordArchivePublication) error {
	// Cascades discard only this artifact's derived rows, in the same transaction.
	if _, err := d.db.Exec(ctx, `DELETE FROM archive_sources WHERE path=?`, p.Path); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `INSERT INTO archive_sources(
		path,root_id,course_id,fingerprint,sha256,channel_id,exported_at,status,indexed_at,object_id)
		VALUES(?,?,?,?,?,?,?,'ready',strftime('%Y-%m-%d %H:%M:%S','now'),?)`,
		p.Path, fmt.Sprint(p.RootID), p.CourseID, p.ObjectID,
		p.ObjectID, p.ChannelID, p.ExportedAt, p.ObjectID); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `INSERT INTO channels(
		channel_id,root_id,course_id,guild_id,channel_name,channel_type,
		parent_channel_id,topic,source_path,exported_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(channel_id) DO UPDATE SET root_id=excluded.root_id,
			course_id=excluded.course_id,guild_id=excluded.guild_id,
			channel_name=excluded.channel_name,channel_type=excluded.channel_type,
			parent_channel_id=excluded.parent_channel_id,topic=excluded.topic,
			source_path=excluded.source_path,exported_at=excluded.exported_at`,
		p.ChannelID, p.RootID, p.CourseID, p.GuildID, p.ChannelName,
		p.ChannelType, p.ParentChannelID, p.ChannelTopic, p.Path, p.ExportedAt); err != nil {
		return err
	}
	_, err := d.db.Exec(ctx, `INSERT INTO messages(
		message_id,channel_id,course_id,timestamp,timestamp_epoch,author_key,author_name,
		content,searchable_text,reply_to_message_id,message_type,is_pinned,
		reaction_count,attachment_metadata_json,source_path)
		SELECT message_id,?,?,timestamp,timestamp_epoch,author_key,author_name,
			content,searchable_text,reply_to_message_id,message_type,is_pinned,
			reaction_count,attachment_metadata_json,?
		FROM tree_discord_stage ORDER BY message_id`,
		p.ChannelID, p.CourseID, p.Path)
	return err
}

func (d sqliteDiscordImports) PublishConversation(ctx context.Context, c database.DiscordConversation) error {
	if _, err := d.db.Exec(ctx, `INSERT INTO conversations(
		conversation_id,course_id,root_id,channel_id,channel_name,channel_type,
		first_message_id,last_message_id,started_at,ended_at,ended_at_epoch,
		text,normalized_text,participant_count,reaction_count,is_pinned,metadata_json,source_path)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.ID, c.CourseID, c.RootID, c.ChannelID, c.ChannelName, c.ChannelType,
		c.FirstMessageID, c.LastMessageID, c.StartedAt, c.EndedAt, c.EndedAtEpoch,
		c.Text, c.NormalizedText, c.ParticipantCount, c.ReactionCount,
		c.Pinned, c.MetadataJSON, c.SourcePath); err != nil {
		return err
	}
	for position, messageID := range c.MessageIDs {
		if _, err := d.db.Exec(ctx, `INSERT INTO conversation_messages(
			conversation_id,message_id,source_path,position) VALUES(?,?,?,?)`,
			c.ID, messageID, c.SourcePath, position); err != nil {
			return err
		}
	}
	if _, err := d.db.Exec(ctx, `INSERT INTO conversations_fts(
		conversation_id,text,normalized_text,channel_name) VALUES(?,?,?,?)`,
		c.ID, c.Text, c.NormalizedText, c.ChannelName); err != nil {
		return err
	}
	_, err := d.db.Exec(ctx, `INSERT INTO conversation_embeddings(
		conversation_id,model,vector,dimensions) VALUES(?,?,?,?)`,
		c.ID, c.EmbeddingModel, c.EmbeddingVector, c.EmbeddingDimensions)
	return err
}

func (d sqliteDiscordImports) RegisterArchiveMedia(
	ctx context.Context, sourcePath, relativePath, objectID string,
) error {
	_, err := d.db.Exec(ctx, `INSERT INTO archive_media(
		source_path,relative_path,object_id) VALUES(?,?,?)`,
		sourcePath, relativePath, objectID)
	return err
}

func (d sqliteDiscordImports) ArchiveMedia(
	ctx context.Context, sourcePath string, limit int,
) ([]database.DiscordMediaEntry, error) {
	rows, err := d.db.Query(ctx, `SELECT am.relative_path,
		o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM archive_media am JOIN objects o ON o.id=am.object_id
		WHERE am.source_path=? ORDER BY am.relative_path LIMIT ?`, sourcePath, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.DiscordMediaEntry{}
	for rows.Next() {
		var entry database.DiscordMediaEntry
		if err := rows.Scan(&entry.RelativePath,
			&entry.Object.Bucket, &entry.Object.Key, &entry.Object.VersionID,
			&entry.Object.SHA256, &entry.Object.Bytes, &entry.Object.MediaType); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func (d sqliteDiscordImports) MediaInventory(
	ctx context.Context, sourcePath string, limit int,
) ([]database.DiscordMediaItem, error) {
	rows, err := d.db.Query(ctx, `SELECT relative_path,object_id
		FROM archive_media WHERE source_path=? ORDER BY relative_path LIMIT ?`,
		sourcePath, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.DiscordMediaItem{}
	for rows.Next() {
		var item database.DiscordMediaItem
		if err := rows.Scan(&item.RelativePath, &item.ObjectID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d sqliteDiscordImports) ReadyMediaObject(ctx context.Context, id string) (database.ObjectReference, error) {
	var object database.ObjectReference
	err := d.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM objects o WHERE o.id=? AND EXISTS(
		SELECT 1 FROM archive_media am
		JOIN archive_sources a ON a.path=am.source_path AND a.status='ready'
		JOIN discord_course_channels mapping
			ON mapping.root_channel_id=a.root_id AND mapping.course_id=a.course_id
		JOIN courses c ON c.id=a.course_id AND c.hidden=0
		WHERE am.object_id=o.id)`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID,
			&object.SHA256, &object.Bytes, &object.MediaType)
	return object, err
}

func (d sqliteDiscordImports) ExportCursorForUpdate(ctx context.Context, rootID, channelID int64) (int64, error) {
	// The admitted writer owns the row until commit/rollback; no row-lock
	// clause exists on this backend.
	var after int64
	err := d.db.QueryRow(ctx, `SELECT after_id FROM export_cursors
		WHERE root_id=? AND channel_id=?`, rootID, channelID).Scan(&after)
	return after, err
}

func (d sqliteDiscordImports) AdvanceExportCursor(ctx context.Context, cursor database.DiscordExportCursor) error {
	_, err := d.db.Exec(ctx, `INSERT INTO export_cursors(
		root_id,channel_id,after_id,next_at) VALUES(?,?,?,?)
		ON CONFLICT(root_id,channel_id) DO UPDATE SET after_id=excluded.after_id,
			next_at=excluded.next_at,error=NULL`,
		cursor.RootID, cursor.ChannelID, cursor.AfterID, sqliteTimestamp(cursor.NextAt))
	return err
}

func (d sqliteDiscordImports) MismappedSource(ctx context.Context) (database.DiscordMappedSource, error) {
	var source database.DiscordMappedSource
	err := d.db.QueryRow(ctx, `SELECT a.path,CAST(a.root_id AS INTEGER),a.channel_id,m.course_id,
		o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM archive_sources a
		JOIN discord_course_channels m ON m.root_channel_id=a.root_id
		JOIN objects o ON o.id=a.object_id
		WHERE a.course_id<>m.course_id ORDER BY a.indexed_at,a.path LIMIT 1`).
		Scan(&source.Path, &source.RootID, &source.ChannelID, &source.CourseID,
			&source.Object.Bucket, &source.Object.Key, &source.Object.VersionID,
			&source.Object.SHA256, &source.Object.Bytes, &source.Object.MediaType)
	return source, err
}
