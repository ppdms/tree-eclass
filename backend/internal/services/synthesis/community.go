package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"tree-eclass/internal/infrastructure/rdbms"
	"unicode/utf8"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/messages"
)

var communityQueries = []string{
	"εξετάσεις εξεταστέα ύλη παλιά θέματα προηγούμενα θέματα exam scope past questions",
	"ασκήσεις λύσεις λυμένες εργασίες φροντιστήριο exercises solutions assignments",
	"οδηγός μελέτης σημειώσεις περίληψη sos διάβασμα study guide notes summary",
}

func collectCommunity(ctx context.Context, tx rdbms.Tx, course int64) ([]map[string]any, []any, error) {
	ids, err := searchCommunityIDs(ctx, tx, course)
	if err != nil {
		return nil, nil, err
	}
	return loadCommunityEntries(ctx, tx, course, ids)
}

// communitySearchSQL finds recent ready conversations matching every term of
// one community query. CAST keeps the text search portable: the FTS vector
// column has no common type across backends.
const communitySearchSQL = `SELECT c.conversation_id FROM messages.conversations c
JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text AND mapping.course_id=c.course_id
JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=c.root_id::text AND a.status='ready'
JOIN messages.conversations_fts f USING(conversation_id)
WHERE c.course_id=$1 AND `

// searchCommunityIDs runs each community query and returns the deduplicated
// conversation ids in first-hit order.
func searchCommunityIDs(ctx context.Context, tx rdbms.Tx, course int64) ([]string, error) {
	ids := []string{}
	seen := map[string]bool{}
	for _, query := range communityQueries {
		terms := strings.Fields(identity.Search(query))
		if len(terms) == 0 {
			continue
		}
		conditions := make([]string, 0, len(terms))
		args := make([]any, 0, len(terms)+1)
		args = append(args, course)
		for i, term := range terms {
			escaped := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(term, `\`, `\\`), `%`, `\%`), `_`, `\_`)
			conditions = append(conditions, fmt.Sprintf(`f.search_vector::text LIKE $%d ESCAPE '\'`, i+2))
			args = append(args, "%"+escaped+"%")
		}
		rows, err := tx.Query(ctx, communitySearchSQL+strings.Join(conditions, " AND ")+`
ORDER BY c.ended_at_epoch DESC,c.conversation_id LIMIT 8`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// loadCommunityEntries loads the evidence entries for the collected ids
// within the 20000-rune budget, capturing each content hash.
func loadCommunityEntries(
	ctx context.Context, tx rdbms.Tx, course int64, ids []string,
) ([]map[string]any, []any, error) {
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
func communityEntry(ctx context.Context, tx rdbms.Tx, course int64, id string) (map[string]any, error) {
	var name, ended, channel string
	var guild *int64
	err := tx.QueryRow(ctx, `SELECT c.channel_name,c.ended_at,c.channel_id::text,ch.guild_id FROM messages.conversations c LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id WHERE c.conversation_id=$1 AND c.course_id=$2`, id, course).
		Scan(&name, &ended, &channel, &guild)
	if err != nil {
		return nil, err
	}
	items, ids, urls, err := scanCommunityMessages(ctx, tx, course, id, channel, guild)
	if err != nil {
		return nil, err
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
	}, err
}

// scanCommunityMessages loads the capped message excerpt rows for one
// community entry, decoding identity-encoded text and building discord urls.
func scanCommunityMessages(
	ctx context.Context, tx rdbms.Tx, course int64, id, channel string, guild *int64,
) ([]any, []any, []any, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT m.message_id::text,m.timestamp,substr(m.author_name,1,100),substr(m.content,1,500) FROM messages.conversation_messages cm JOIN messages.messages m USING(source_path,message_id)
 WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position,m.message_id LIMIT 8`,
		id,
		course,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	items, ids, urls := []any{}, []any{}, []any{}
	for rows.Next() {
		var mid, at, author, content string
		if err = rows.Scan(&mid, &at, &author, &content); err != nil {
			return nil, nil, nil, err
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
	return items, ids, urls, rows.Err()
}
