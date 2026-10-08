package messages

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
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
	tx rdbms.Tx,
	id string,
	c *Conversation,
) (channel, first, last int64, guild *int64, err error) {
	err = tx.QueryRow(ctx, `SELECT v.conversation_id,v.course_id,c.name,c.short_name,v.channel_id,v.channel_name,v.channel_type,v.started_at,v.ended_at,v.first_message_id,v.last_message_id,ch.guild_id
 FROM messages.conversations v JOIN app.courses c ON c.id=v.course_id AND c.hidden=0
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=CAST(v.root_id AS TEXT) AND mapping.course_id=v.course_id
 JOIN messages.archive_sources a ON a.path=v.source_path AND a.course_id=v.course_id AND a.root_id=CAST(v.root_id AS TEXT) AND a.status='ready'
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
	tx rdbms.Tx,
	guild *int64,
	courseID, channel, first, last int64,
	before, after int,
) ([]Message, []Message, error) {
	older, err := readMessages(
		ctx,
		tx,
		contextMessagesQuery+` AND m.channel_id=$2 AND m.message_id<$3 ORDER BY m.message_id DESC,a.indexed_at DESC LIMIT $4) ranked`,
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
		contextMessagesQuery+` AND m.channel_id=$2 AND m.message_id>$3 ORDER BY m.message_id,a.indexed_at DESC LIMIT $4) ranked`,
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
	tx rdbms.Tx,
	guild *int64,
	courseID int64,
	messages []Message,
) ([]Message, error) {
	return readMessages(
		ctx,
		tx,
		contextMessagesQuery+` AND m.message_id=ANY($2::bigint[]) AND ch.guild_id IS NOT DISTINCT FROM $3::bigint ORDER BY m.message_id,a.indexed_at DESC LIMIT 200) ranked`,
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
// course. Message rows scan into messageRow and assemble in Go so the queries
// stay portable: to_jsonb(row) has no sqlite form and fails at prepare time.
// Root IDs compare through CAST to text (channel roots are text on the
// mapping side on both schemas); DISTINCT ON becomes ROW_NUMBER over the
// message id ordered by newest export, and guild comparison keeps
// IS NOT DISTINCT FROM out of the text.
const messageFields = `CAST(m.message_id AS TEXT) message_id,CAST(m.channel_id AS TEXT) channel_id,m.timestamp,m.author_name,substr(m.content,1,40000) content,
 CAST(m.reply_to_message_id AS TEXT) reply_to_message_id,m.message_type,CASE WHEN m.is_pinned<>0 THEN 1 ELSE 0 END is_pinned,m.reaction_count,
 CASE WHEN octet_length(m.attachment_metadata_json)<=65536 THEN m.attachment_metadata_json ELSE '[]' END attachments,
 CASE WHEN length(m.content)>40000 OR octet_length(m.attachment_metadata_json)>65536 THEN 1 ELSE 0 END truncated`

const contextMessagesQuery = `SELECT message_id,channel_id,timestamp,author_name,content,reply_to_message_id,message_type,is_pinned,reaction_count,attachments,truncated,dedup FROM(SELECT ` + messageFields + `,ROW_NUMBER() OVER(PARTITION BY m.message_id ORDER BY a.indexed_at DESC) dedup
 FROM messages.messages m JOIN messages.archive_sources a ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
 JOIN messages.channels ch ON ch.channel_id=m.channel_id AND ch.course_id=m.course_id
 WHERE m.course_id=$1`
const conversationMessagesQuery = `SELECT ` + messageFields + `
 FROM messages.conversation_messages cm JOIN messages.messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path
 JOIN messages.archive_sources a ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
 WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position LIMIT 201`

// messageRow holds one explicit-column message row. Pointers preserve NULLs
// the way to_jsonb nulls did; attachments stay JSON text until decode. The
// context query carries a ROW_NUMBER dedup column; the conversation query has
// none, so the scanner takes it only on context-shaped rows.
type messageRow struct {
	id, channel, timestamp, author, content, reply, kind string
	replySet                                             bool
	pinned, truncated                                    int64
	reactions                                            int64
	attachments                                          string
	dedup                                                int64
}

func scanMessageRow(rows rdbms.Rows, row *messageRow, withDedup bool) error {
	var reply *string
	var attachments []byte
	base := []any{
		&row.id, &row.channel, &row.timestamp, &row.author, &row.content,
		&reply, &row.kind, &row.pinned, &row.reactions, &attachments, &row.truncated,
	}
	if withDedup {
		base = append(base, &row.dedup)
	} else {
		row.dedup = 1
	}
	if err := rows.Scan(base...); err != nil {
		return err
	}
	if reply != nil {
		row.reply, row.replySet = *reply, true
	}
	row.attachments = string(attachments)
	return nil
}

// assembleMessage converts one scanned row to the Message shape the old
// to_jsonb(rows) transport decoded. IDs stay text; NULL reply_to stays nil
// (the old JSON null); attachments parse from their JSON text. Integer flags
// become bools here so both drivers can return them as plain integers.
func assembleMessage(row messageRow) (Message, error) {
	m := Message{
		ID:        row.id,
		Channel:   row.channel,
		Timestamp: row.timestamp,
		Author:    row.author,
		Content:   row.content,
		Type:      row.kind,
		Pinned:    row.pinned != 0,
		Reactions: row.reactions,
		Truncated: row.truncated != 0,
	}
	if row.replySet {
		reply := row.reply
		m.Reply = &reply
	}
	raw := row.attachments
	if raw == "" {
		raw = "[]"
	}
	if err := decode([]byte(raw), &m.Attachments); err != nil {
		return Message{}, err
	}
	return m, nil
}

func readMessages(ctx context.Context, tx rdbms.Tx, query string, guild *int64, args ...any) ([]Message, error) {
	// The context query text starts with the shared contextMessagesQuery
	// prefix; the conversation query does not. The prefix carries the dedup
	// column, so scan it only there and keep the newest row per message.
	withDedup := len(query) >= len(contextMessagesQuery) && query[:len(contextMessagesQuery)] == contextMessagesQuery
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Message{}
	for rows.Next() {
		var row messageRow
		if err = scanMessageRow(rows, &row, withDedup); err != nil {
			return nil, err
		}
		if withDedup && row.dedup != 1 {
			continue
		}
		m, err := assembleMessage(row)
		if err != nil {
			return nil, err
		}
		decodeMessage(&m, guild)
		result = append(result, m)
	}
	return result, rows.Err()
}
