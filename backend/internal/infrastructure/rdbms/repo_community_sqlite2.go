package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func (c sqliteCommunity) LexicalCandidates(ctx context.Context, ids []int64, terms []string,
	limit int) ([]database.CommunityCandidate, error) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return communityLexicalCandidates(ctx, c.db, `SELECT `+communityHitColumnsSQLite+
		`,substr(c.text,1,400) excerpt,c.text FROM conversations c`+communityConvSourcesSQLite+
		` JOIN conversations_fts f ON f.conversation_id=c.conversation_id`+
		` WHERE c.course_id IN`+sqlitePlaceholders(len(ids))+
		` ORDER BY c.ended_at_epoch DESC,c.conversation_id`, args, terms, limit)
}

func (c sqliteCommunity) SemanticCandidates(ctx context.Context, ids []int64, lexical []string,
	model string, dimensions int) (database.Iterator[database.CommunityEmbeddedCandidate], error) {
	if len(ids) == 0 {
		rows, err := c.db.Query(ctx, `SELECT `+communityHitColumnsSQLite+`,substr(c.text,1,1200) excerpt,`+
			`e.vector FROM conversations c`+communityConvSourcesSQLite+
			` JOIN conversation_embeddings e`+
			` ON e.conversation_id=c.conversation_id AND e.model=? AND e.dimensions=?`+
			` WHERE 0`, model, dimensions)
		if err != nil {
			return nil, err
		}
		return typedIterator(rows, scanCommunityEmbedded), nil
	}
	recentArgs := make([]any, 0, len(ids)+1)
	for _, id := range ids {
		recentArgs = append(recentArgs, id)
	}
	recentArgs = append(recentArgs, 5000)
	lexicalArgs := make([]any, 0, len(lexical))
	union, lexicalArgs := communitySemanticUnionSQLite(lexicalArgs, lexical)
	finalArgs := make([]any, 0, len(ids)+2+len(ids))
	finalArgs = append(finalArgs, lexicalArgs...)
	finalArgs = append(finalArgs, model, dimensions)
	for _, id := range ids {
		finalArgs = append(finalArgs, id)
	}
	query := `WITH recent AS (SELECT c.conversation_id FROM conversations c` +
		communityConvSourcesSQLite + ` WHERE c.course_id IN` + sqlitePlaceholders(len(ids)) +
		` ORDER BY c.ended_at_epoch DESC,c.conversation_id LIMIT ?),` +
		` candidates AS (` + union + `)` +
		` SELECT ` + communityHitColumnsSQLite + `,substr(c.text,1,1200) excerpt,e.vector FROM candidates` +
		` JOIN conversations c ON c.conversation_id=candidates.conversation_id` +
		` JOIN courses co ON co.id=c.course_id AND co.hidden=0` +
		` JOIN discord_course_channels mapping` +
		` ON mapping.root_channel_id=CAST(c.root_id AS TEXT) AND mapping.course_id=c.course_id` +
		` JOIN archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id` +
		` AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'` +
		` LEFT JOIN channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id` +
		` JOIN conversation_embeddings e ON e.conversation_id=c.conversation_id` +
		` AND e.model=? AND e.dimensions=? AND length(e.vector)=1536` +
		` WHERE c.course_id IN` + sqlitePlaceholders(len(ids))
	args := append(recentArgs, finalArgs...)
	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanCommunityEmbedded), nil
}

func communitySemanticUnionSQLite(selected []any, lexical []string) (string, []any) {
	seen := map[string]bool{}
	unique := []string{}
	for _, id := range lexical {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	union := "SELECT conversation_id FROM recent"
	if len(unique) == 0 {
		return union, selected
	}
	holders := make([]string, len(unique))
	for i, id := range unique {
		if i == 0 {
			holders[i] = "? AS conversation_id"
		} else {
			holders[i] = "?"
		}
		selected = append(selected, id)
	}
	union += " UNION SELECT conversation_id FROM (SELECT " +
		joinPlaceholders(holders, " UNION ALL SELECT ") + ")"
	return union, selected
}

func (c sqliteCommunity) ConversationMemberIDs(ctx context.Context, id string) ([]string, error) {
	rows, err := c.db.Query(ctx, `SELECT CAST(message_id AS TEXT) FROM conversation_messages`+
		` WHERE conversation_id=? ORDER BY position LIMIT 201`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		out = append(out, member)
	}
	if out == nil {
		out = []string{}
	}
	return out, rows.Err()
}

func (c sqliteCommunity) ConversationMemberCount(ctx context.Context, id string) (int64, error) {
	var count int64
	err := c.db.QueryRow(ctx, `SELECT count(*) FROM conversation_messages`+
		` WHERE conversation_id=?`, id).Scan(&count)
	return count, err
}

func (c sqliteCommunity) SnapshotConversation(ctx context.Context, course int64,
	id string) (database.CommunityConversationHash, error) {
	var out database.CommunityConversationHash
	err := c.db.QueryRow(ctx, `SELECT c.conversation_id,c.course_id,c.root_id,c.channel_id,`+
		`c.channel_name,c.channel_type,c.first_message_id,c.last_message_id,`+
		`c.started_at,c.ended_at,c.ended_at_epoch,c.text,c.normalized_text,`+
		`c.participant_count,c.reaction_count,c.is_pinned,c.metadata_json,c.source_path,a.fingerprint`+
		` FROM conversations c JOIN courses co ON co.id=c.course_id`+
		` JOIN discord_course_channels mapping`+
		` ON mapping.root_channel_id=CAST(c.root_id AS TEXT) AND mapping.course_id=c.course_id`+
		` JOIN archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id`+
		` AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'`+
		` WHERE c.course_id=? AND c.conversation_id=?`+
		` AND (co.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p`+
		` WHERE p.course_id=co.id AND p.enabled=1))`, course, id).Scan(
		&out.ConversationID, &out.CourseID, &out.RootID, &out.ChannelID, &out.ChannelName,
		&out.ChannelType, &out.FirstMessageID, &out.LastMessageID, &out.StartedAt, &out.EndedAt,
		&out.EndedAtEpoch, &out.Text, &out.NormalizedText, &out.ParticipantCount, &out.ReactionCount,
		&out.Pinned, &out.MetadataJSON, &out.SourcePath, &out.Fingerprint)
	return out, err
}

func (c sqliteCommunity) SnapshotMembers(ctx context.Context, id string) ([]database.CommunityMessageHash, error) {
	rows, err := c.db.Query(ctx, `SELECT cm.message_id,cm.source_path,cm.position,`+
		`m.message_id,m.message_id,m.channel_id,m.course_id,m.timestamp,m.timestamp_epoch,`+
		`m.author_key,m.author_name,m.content,m.searchable_text,m.reply_to_message_id,`+
		`m.message_type,m.is_pinned,m.reaction_count,m.attachment_metadata_json,m.source_path`+
		` FROM conversation_messages cm`+
		` LEFT JOIN messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path`+
		` WHERE cm.conversation_id=? ORDER BY cm.position,cm.message_id,cm.source_path`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.CommunityMessageHash{}
	for rows.Next() {
		var row database.CommunityMessageHash
		var probe *int64
		if err := rows.Scan(&row.LinkMessageID, &row.LinkSource, &row.LinkPosition,
			&probe, &row.MessageID, &row.ChannelID, &row.CourseID, &row.Timestamp,
			&row.TimestampEp, &row.AuthorKey, &row.AuthorName, &row.Content, &row.SearchText,
			&row.ReplyTo, &row.MessageType, &row.Pinned, &row.Reactions, &row.Attachments,
			&row.SourcePath); err != nil {
			return nil, err
		}
		if probe == nil {
			row.MessageID = nil
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
