package rdbms

import (
	"tree-eclass/internal/domain/database"
)

// communitySourcesPG scopes message reads to currently mapped courses with
// ready archive ancestry. Alias m carries the message row. Root IDs compare
// through text (channel roots are text on the mapping side on both schemas).
const communitySourcesPG = ` JOIN messages.archive_sources a` +
	` ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'` +
	` JOIN app.discord_course_channels mapping` +
	` ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id` +
	` JOIN messages.channels ch ON ch.channel_id=m.channel_id AND ch.course_id=m.course_id`

const communityConvSourcesPG = ` JOIN app.courses co ON co.id=c.course_id AND co.hidden=0` +
	` JOIN app.discord_course_channels mapping` +
	` ON mapping.root_channel_id=CAST(c.root_id AS TEXT) AND mapping.course_id=c.course_id` +
	` JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id` +
	` AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'` +
	` LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id`

// communityHitColumnsPG selects every CommunityCandidate field in scan order
// with the excerpt slot appended by each caller.
const communityHitColumnsPG = `c.conversation_id,c.course_id,co.name course_name,` +
	`co.short_name course_short_name,CAST(c.channel_id AS TEXT) channel_id,` +
	`c.channel_name,c.channel_type,CAST(c.first_message_id AS TEXT) first_message_id,` +
	`CAST(c.last_message_id AS TEXT) last_message_id,c.started_at,c.ended_at,` +
	`c.ended_at_epoch,c.participant_count,c.reaction_count,CAST(c.is_pinned AS BIGINT) is_pinned,ch.guild_id`

// communityMessageColumnsPG selects every CommunityMessage field in scan
// order. Content carries 40000 characters, attachments carry stored JSON or
// "[]" past 65536 bytes, and the truncated flag mirrors that boundary.
const communityMessageColumnsPG = `CAST(m.message_id AS TEXT) message_id,` +
	`CAST(m.channel_id AS TEXT) channel_id,m.timestamp,m.author_name,` +
	`substr(m.content,1,40000) content,CAST(m.reply_to_message_id AS TEXT) reply_to_message_id,` +
	`m.message_type,CASE WHEN m.is_pinned<>0 THEN 1 ELSE 0 END is_pinned,m.reaction_count,` +
	`CASE WHEN octet_length(m.attachment_metadata_json)<=65536` +
	` THEN m.attachment_metadata_json ELSE '[]' END attachments,` +
	`CASE WHEN length(m.content)>40000 OR octet_length(m.attachment_metadata_json)>65536` +
	` THEN 1 ELSE 0 END truncated`

func scanCommunityMessage(rows nativeRows) (database.CommunityMessage, error) {
	var out database.CommunityMessage
	var pinned, truncated int64
	var attachments []byte
	err := rows.Scan(&out.MessageID, &out.ChannelID, &out.Timestamp, &out.AuthorName, &out.Content,
		&out.ReplyTo, &out.MessageType, &pinned, &out.ReactionCount, &attachments, &truncated)
	if err != nil {
		return out, err
	}
	out.Pinned, out.Truncated = pinned != 0, truncated != 0
	out.AttachmentsJSON = string(attachments)
	return out, nil
}

func scanCommunityCandidate(rows nativeRows, extra ...any) (database.CommunityCandidate, error) {
	var out database.CommunityCandidate
	var pinned int64
	dest := []any{&out.ConversationID, &out.CourseID, &out.CourseName, &out.CourseShortName,
		&out.ChannelID, &out.ChannelName, &out.ChannelType, &out.FirstMessageID, &out.LastMessageID,
		&out.StartedAt, &out.EndedAt, &out.EndedAtEpoch, &out.Participants, &out.Reactions,
		&pinned, &out.GuildID, &out.Excerpt}
	dest = append(dest, extra...)
	if err := rows.Scan(dest...); err != nil {
		return out, err
	}
	out.Pinned = pinned != 0
	return out, nil
}

func scanCommunityEmbedded(rows nativeRows) (database.CommunityEmbeddedCandidate, error) {
	var out database.CommunityEmbeddedCandidate
	item, err := scanCommunityCandidate(rows, &out.Vector)
	out.CommunityCandidate = item
	return out, err
}

func joinPlaceholders(holders []string, sep string) string {
	joined := ""
	for i, holder := range holders {
		if i > 0 {
			joined += sep
		}
		joined += holder
	}
	return joined
}
