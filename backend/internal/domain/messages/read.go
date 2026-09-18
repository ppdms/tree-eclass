package messages

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

func (s Reader) Read(ctx context.Context, id string, before, after int) (Reading, error) {
	result := Reading{
		Messages: []Message{},
		Replies:  []Message{},
		Before:   []Message{},
		After:    []Message{},
		Notice:   CommunityNotice,
	}
	if id == "" || len(id) > 256 || before < 0 || before > 20 || after < 0 || after > 20 {
		return result, errors.New("invalid conversation or context bounds")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	c := &result.Conversation
	channel, first, last, guild, err := s.readConversation(ctx, tx, id, c)
	if err != nil {
		return result, err
	}
	c.Channel, c.Source, c.Evidence = strconv.FormatInt(channel, 10), "discord", "community_discussion"
	c.Metadata = map[string]any{"guild_id": guild}
	result.Messages, err = readMessages(ctx, tx, conversationMessagesQuery, guild, id, c.CourseID)
	if err != nil {
		return result, err
	}
	result.Truncated = len(result.Messages) > 200
	result.Messages = result.Messages[:min(200, len(result.Messages))]
	result.Before, result.After, err = s.readContextSides(
		ctx,
		tx,
		guild,
		c.CourseID,
		channel,
		first,
		last,
		before,
		after,
	)
	if err != nil {
		return result, err
	}
	result.Replies, err = s.readReplies(ctx, tx, guild, c.CourseID, result.Messages)
	if err != nil {
		return result, err
	}
	result.foldTruncated()
	return result, tx.Commit(ctx)
}

func (s Reader) readConversation(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	c *Conversation,
) (channel, first, last int64, guild *int64, err error) {
	err = tx.QueryRow(ctx, `SELECT v.conversation_id,v.course_id,c.name,c.short_name,v.channel_id,v.channel_name,v.channel_type,v.started_at,v.ended_at,v.first_message_id,v.last_message_id,ch.guild_id
 FROM messages.conversations v JOIN app.courses c ON c.id=v.course_id AND c.hidden=0
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=v.root_id::text AND mapping.course_id=v.course_id
 JOIN messages.archive_sources a ON a.path=v.source_path AND a.course_id=v.course_id AND a.root_id=v.root_id::text AND a.status='ready'
 LEFT JOIN messages.channels ch ON ch.channel_id=v.channel_id AND ch.course_id=v.course_id
 WHERE v.conversation_id=$1`, id).
		Scan(
			&c.ID,
			&c.CourseID,
			&c.CourseName,
			&c.CourseShortName,
			&channel,
			&c.Name,
			&c.Kind,
			&c.Started,
			&c.Ended,
			&first,
			&last,
			&guild,
		)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	for _, value := range []*string{&c.CourseName, c.CourseShortName, &c.Name} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	return channel, first, last, guild, nil
}

func (s Reader) readContextSides(
	ctx context.Context,
	tx pgx.Tx,
	guild *int64,
	courseID, channel, first, last int64,
	before, after int,
) ([]Message, []Message, error) {
	older, err := readMessages(
		ctx,
		tx,
		contextMessagesQuery+` AND m.channel_id=$2 AND m.message_id<$3 ORDER BY m.message_id DESC,a.indexed_at DESC LIMIT $4) rows`,
		guild,
		courseID,
		channel,
		first,
		before,
	)
	if err != nil {
		return nil, nil, err
	}
	slices.Reverse(older)
	newer, err := readMessages(
		ctx,
		tx,
		contextMessagesQuery+` AND m.channel_id=$2 AND m.message_id>$3 ORDER BY m.message_id,a.indexed_at DESC LIMIT $4) rows`,
		guild,
		courseID,
		channel,
		last,
		after,
	)
	return older, newer, err
}

func (s Reader) readReplies(
	ctx context.Context,
	tx pgx.Tx,
	guild *int64,
	courseID int64,
	messages []Message,
) ([]Message, error) {
	return readMessages(
		ctx,
		tx,
		contextMessagesQuery+` AND m.message_id=ANY($2::bigint[]) AND ch.guild_id IS NOT DISTINCT FROM $3::bigint ORDER BY m.message_id,a.indexed_at DESC LIMIT 200) rows`,
		guild,
		courseID,
		replyIDs(messages),
		guild,
	)
}

func replyIDs(messages []Message) []int64 {
	replies := []int64{}
	for _, m := range messages {
		if m.Reply != nil {
			value, err := strconv.ParseInt(*m.Reply, 10, 64)
			if err == nil && !slices.Contains(replies, value) {
				replies = append(replies, value)
			}
		}
	}
	return replies
}

func (r *Reading) foldTruncated() {
	for _, rows := range [][]Message{r.Messages, r.Before, r.After, r.Replies} {
		for _, m := range rows {
			r.Truncated = r.Truncated || m.Truncated
		}
	}
}

// All context and reply reads remain inside the same currently mapped visible
// course. Deduplication selects the newest export for overlapping archive files.
const messageFields = `m.message_id::text message_id,m.channel_id::text channel_id,m.timestamp,m.author_name,left(m.content,40000) content,
 m.reply_to_message_id::text reply_to_message_id,m.message_type,m.is_pinned<>0 is_pinned,m.reaction_count,
 CASE WHEN octet_length(m.attachment_metadata_json)<=65536 THEN m.attachment_metadata_json::jsonb ELSE '[]'::jsonb END attachments,
 (length(m.content)>40000 OR octet_length(m.attachment_metadata_json)>65536) truncated`

const contextMessagesQuery = `SELECT to_jsonb(rows) FROM(SELECT DISTINCT ON(m.message_id) ` + messageFields + `
 FROM messages.messages m JOIN messages.archive_sources a ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
 JOIN messages.channels ch ON ch.channel_id=m.channel_id AND ch.course_id=m.course_id
 WHERE m.course_id=$1`

const conversationMessagesQuery = `SELECT to_jsonb(rows) FROM(SELECT ` + messageFields + `
 FROM messages.conversation_messages cm JOIN messages.messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path
 JOIN messages.archive_sources a ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
 WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position LIMIT 201) rows`

func readMessages(ctx context.Context, tx pgx.Tx, query string, guild *int64, args ...any) ([]Message, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Message{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var m Message
		if err = decode(raw, &m); err != nil {
			return nil, err
		}
		decodeMessage(&m, guild)
		result = append(result, m)
	}
	return result, rows.Err()
}
