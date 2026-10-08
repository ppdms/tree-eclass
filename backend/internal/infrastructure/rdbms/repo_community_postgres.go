package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresCommunity struct{ db nativeDBTX }

func (c postgresCommunity) Conversation(ctx context.Context, id string) (database.CommunityConversation, error) {
	var out database.CommunityConversation
	err := c.db.QueryRow(ctx, `SELECT v.conversation_id,v.course_id,c.name,c.short_name,`+
		`v.channel_id,v.channel_name,v.channel_type,v.started_at,v.ended_at,`+
		`v.first_message_id,v.last_message_id,ch.guild_id`+
		` FROM messages.conversations v JOIN app.courses c ON c.id=v.course_id AND c.hidden=0`+
		` JOIN app.discord_course_channels mapping`+
		` ON mapping.root_channel_id=CAST(v.root_id AS TEXT) AND mapping.course_id=v.course_id`+
		` JOIN messages.archive_sources a ON a.path=v.source_path AND a.course_id=v.course_id`+
		` AND a.root_id=CAST(v.root_id AS TEXT) AND a.status='ready'`+
		` LEFT JOIN messages.channels ch ON ch.channel_id=v.channel_id AND ch.course_id=v.course_id`+
		` WHERE v.conversation_id=$1`, id).Scan(&out.ID, &out.CourseID, &out.CourseName,
		&out.CourseShortName, &out.ChannelID, &out.ChannelName, &out.ChannelType,
		&out.StartedAt, &out.EndedAt, &out.FirstMessageID, &out.LastMessageID, &out.GuildID)
	return out, err
}

func (c postgresCommunity) ConversationMessages(ctx context.Context, id string,
	courseID int64) ([]database.CommunityMessage, error) {
	rows, err := c.db.Query(ctx, `SELECT `+communityMessageColumnsPG+
		` FROM messages.conversation_messages cm`+
		` JOIN messages.messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path`+
		` JOIN messages.archive_sources a`+
		` ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'`+
		` JOIN app.discord_course_channels mapping`+
		` ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id`+
		` WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position LIMIT 201`, id, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.CommunityMessage{}
	for rows.Next() {
		item, err := scanCommunityMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (c postgresCommunity) OlderContext(ctx context.Context,
	params database.CommunityContextParams) ([]database.CommunityMessage, error) {
	return c.communityContext(ctx, params, true)
}

func (c postgresCommunity) NewerContext(ctx context.Context,
	params database.CommunityContextParams) ([]database.CommunityMessage, error) {
	return c.communityContext(ctx, params, false)
}

func (c postgresCommunity) communityContext(ctx context.Context, params database.CommunityContextParams,
	older bool) ([]database.CommunityMessage, error) {
	compare, order := ">", "m.message_id,a.indexed_at DESC"
	if older {
		compare, order = "<", "m.message_id DESC,a.indexed_at DESC"
	}
	rows, err := c.db.Query(ctx, `SELECT message_id,channel_id,timestamp,author_name,content,`+
		`reply_to_message_id,message_type,is_pinned,reaction_count,attachments,truncated`+
		` FROM(SELECT `+communityMessageColumnsPG+
		`,ROW_NUMBER() OVER(PARTITION BY m.message_id ORDER BY a.indexed_at DESC) dedup`+
		` FROM messages.messages m`+communitySourcesPG+
		` WHERE m.course_id=$1 AND m.channel_id=$2 AND m.message_id`+compare+`$3`+
		` ORDER BY `+order+` LIMIT $4) ranked WHERE dedup=1`,
		params.CourseID, params.ChannelID, params.EdgeID, params.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.CommunityMessage{}
	for rows.Next() {
		item, err := scanCommunityMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (c postgresCommunity) ReplyMessages(ctx context.Context, courseID int64,
	ids []int64) ([]database.CommunityMessage, error) {
	out := []database.CommunityMessage{}
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, courseID)
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := c.db.Query(ctx, `SELECT message_id,channel_id,timestamp,author_name,content,`+
		`reply_to_message_id,message_type,is_pinned,reaction_count,attachments,truncated`+
		` FROM(SELECT `+communityMessageColumnsPG+
		`,ROW_NUMBER() OVER(PARTITION BY m.message_id ORDER BY a.indexed_at DESC) dedup`+
		` FROM messages.messages m`+communitySourcesPG+
		` WHERE m.course_id=$1 AND m.message_id IN`+pgPlaceholders(2, len(ids))+
		` ORDER BY m.message_id,a.indexed_at DESC LIMIT 200) ranked WHERE dedup=1`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanCommunityMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (c postgresCommunity) VisibleCourseIDs(ctx context.Context) ([]int64, error) {
	rows, err := c.db.Query(ctx, `SELECT id FROM app.courses WHERE hidden=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (c postgresCommunity) CourseStatus(ctx context.Context, ids []int64) ([]database.CommunityCourseStatus, error) {
	out := []database.CommunityCourseStatus{}
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	courses := pgPlaceholders(1, len(ids))
	rows, err := c.db.Query(ctx, `WITH sources AS (`+
		` SELECT a.* FROM messages.archive_sources a JOIN app.discord_course_channels m`+
		` ON m.root_channel_id=a.root_id AND m.course_id=a.course_id WHERE a.course_id IN`+courses+
		`), message_counts AS (`+
		` SELECT m.course_id,count(*) n,max(m.timestamp) latest FROM messages.messages m`+
		` JOIN sources s ON s.path=m.source_path AND s.course_id=m.course_id AND s.status='ready'`+
		` GROUP BY m.course_id`+
		`), conversation_counts AS (`+
		` SELECT c.course_id,count(*) n FROM messages.conversations c`+
		` JOIN sources s ON s.path=c.source_path AND s.course_id=c.course_id`+
		` AND s.root_id=CAST(c.root_id AS TEXT) AND s.status='ready' GROUP BY c.course_id`+
		`), source_counts AS (`+
		` SELECT course_id,count(*) n,count(*) FILTER(WHERE status='failed') failed`+
		` FROM sources GROUP BY course_id`+
		`), mapped AS (SELECT DISTINCT course_id FROM app.discord_course_channels`+
		` WHERE course_id IN`+courses+`)`+
		` SELECT m.course_id,coalesce(mc.n,0),coalesce(cc.n,0),coalesce(sc.n,0),coalesce(sc.failed,0),mc.latest`+
		` FROM mapped m LEFT JOIN message_counts mc USING(course_id)`+
		` LEFT JOIN conversation_counts cc USING(course_id)`+
		` LEFT JOIN source_counts sc USING(course_id) ORDER BY m.course_id`,
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row database.CommunityCourseStatus
		if err := rows.Scan(&row.CourseID, &row.Messages, &row.Conversations, &row.Sources,
			&row.FailedSources, &row.Latest); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
