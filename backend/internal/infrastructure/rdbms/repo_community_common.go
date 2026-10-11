package rdbms

import (
	"tree-eclass/internal/domain/database"
)

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
