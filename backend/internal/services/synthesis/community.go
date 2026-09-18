package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/messages"
)

var communityQueries = []string{
	"εξετάσεις εξεταστέα ύλη παλιά θέματα προηγούμενα θέματα exam scope past questions",
	"ασκήσεις λύσεις λυμένες εργασίες φροντιστήριο exercises solutions assignments",
	"οδηγός μελέτης σημειώσεις περίληψη sos διάβασμα study guide notes summary",
}

func collectCommunity(ctx context.Context, tx pgx.Tx, course int64) ([]map[string]any, []any, error) {
	ids := []string{}
	seen := map[string]bool{}
	for _, query := range communityQueries {
		rows, err := tx.Query(ctx, `SELECT c.conversation_id FROM messages.conversations c
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=c.root_id::text AND a.status='ready'
 JOIN messages.conversations_fts f USING(conversation_id)
 WHERE c.course_id=$1 AND f.search_vector @@ public.tree_query($2)
 ORDER BY ts_rank_cd(f.search_vector,public.tree_query($2)) DESC,c.ended_at_epoch DESC,c.conversation_id LIMIT 8`, course, query)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, nil, err
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, nil, err
		}
	}
	result, captured := []map[string]any{}, []any{}
	budget := 0
	for _, id := range ids {
		item, err := communityEntry(ctx, tx, course, id)
		if err != nil {
			return nil, nil, err
		}
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, nil, err
		}
		size := utf8.RuneCount(raw)
		if budget+size > 20000 {
			break
		}
		budget += size
		hash, err := messages.Snapshot(ctx, tx, course, id)
		if err != nil {
			return nil, nil, err
		}
		result = append(result, item)
		captured = append(captured, map[string]any{"conversation_id": id, "content_hash": hash})
	}
	return result, captured, nil
}
func communityEntry(ctx context.Context, tx pgx.Tx, course int64, id string) (map[string]any, error) {
	var name, ended, channel string
	var guild *int64
	err := tx.QueryRow(ctx, `SELECT c.channel_name,c.ended_at,c.channel_id::text,ch.guild_id FROM messages.conversations c LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id WHERE c.conversation_id=$1 AND c.course_id=$2`, id, course).
		Scan(&name, &ended, &channel, &guild)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		ctx,
		`SELECT m.message_id::text,m.timestamp,left(m.author_name,100),left(m.content,500) FROM messages.conversation_messages cm JOIN messages.messages m USING(source_path,message_id)
 WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position,m.message_id LIMIT 8`,
		id,
		course,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, ids, urls := []any{}, []any{}, []any{}
	for rows.Next() {
		var mid, at, author, content string
		if err = rows.Scan(&mid, &at, &author, &content); err != nil {
			return nil, err
		}
		items = append(
			items,
			map[string]any{
				"message_id":  mid,
				"timestamp":   at,
				"author_name": identity.Decode(author),
				"content":     identity.Decode(content),
			},
		)
		ids = append(ids, mid)
		if guild != nil {
			urls = append(urls, fmt.Sprintf("https://discord.com/channels/%d/%s/%s", *guild, channel, mid))
		}
	}
	return map[string]any{
		"evidence_ref":       "discord:" + id,
		"conversation_id":    id,
		"channel_name":       identity.Decode(name),
		"ended_at":           ended,
		"messages":           items,
		"message_ids":        ids,
		"message_urls":       urls,
		"excerpts_partial":   true,
		"community_reported": true,
		"untrusted_content":  true,
	}, rows.Err()
}
