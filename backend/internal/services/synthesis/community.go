package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/messages"
)

var communityQueries = []string{
	"εξετάσεις εξεταστέα ύλη παλιά θέματα προηγούμενα θέματα exam scope past questions",
	"ασκήσεις λύσεις λυμένες εργασίες φροντιστήριο exercises solutions assignments",
	"οδηγός μελέτης σημειώσεις περίληψη sos διάβασμα study guide notes summary",
}

func collectCommunity(ctx context.Context, tx database.Tx, course int64) ([]map[string]any, []any, error) {
	ids, err := searchCommunityIDs(ctx, tx, course)
	if err != nil {
		return nil, nil, err
	}
	return loadCommunityEntries(ctx, tx, course, ids)
}

// searchCommunityIDs runs each community query and returns the deduplicated
// conversation ids in first-hit order.
func searchCommunityIDs(ctx context.Context, tx database.Tx, course int64) ([]string, error) {
	ids := []string{}
	seen := map[string]bool{}
	for _, query := range communityQueries {
		terms := strings.Fields(identity.Search(query))
		if len(terms) == 0 {
			continue
		}
		hits, err := tx.Synthesis().SearchCommunityIDs(ctx, database.SynthesisCommunityParams{
			CourseID: course, Terms: terms, Limit: 8,
		})
		if err != nil {
			return nil, err
		}
		for _, id := range hits {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

// loadCommunityEntries loads the evidence entries for the collected ids
// within the 20000-rune budget, capturing each content hash.
func loadCommunityEntries(
	ctx context.Context, tx database.Tx, course int64, ids []string,
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

func communityEntry(ctx context.Context, tx database.Tx, course int64, id string) (map[string]any, error) {
	header, err := tx.Synthesis().CommunityEntry(ctx, course, id)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Synthesis().CommunityMessages(ctx, course, id)
	if err != nil {
		return nil, err
	}
	items, ids, urls := scanCommunityMessages(rows, header.ChannelID, header.GuildID)
	return map[string]any{
		"evidence_ref":       "discord:" + id,
		"conversation_id":    id,
		"channel_name":       identity.Decode(header.ChannelName),
		"ended_at":           header.EndedAt,
		"messages":           items,
		"message_ids":        ids,
		"message_urls":       urls,
		"excerpts_partial":   true,
		"community_reported": true,
		"untrusted_content":  true,
	}, nil
}

// scanCommunityMessages renders the capped message excerpt rows for one
// community entry, decoding identity-encoded text and building discord urls.
func scanCommunityMessages(
	rows []database.SynthesisCommunityMessage, channel string, guild *int64,
) ([]any, []any, []any) {
	items, ids, urls := []any{}, []any{}, []any{}
	for _, row := range rows {
		items = append(
			items,
			map[string]any{
				"message_id":  row.MessageID,
				"timestamp":   row.Timestamp,
				"author_name": identity.Decode(row.Author),
				"content":     identity.Decode(row.Content),
			},
		)
		ids = append(ids, row.MessageID)
		if guild != nil {
			urls = append(urls, fmt.Sprintf("https://discord.com/channels/%d/%s/%s", *guild, channel, row.MessageID))
		}
	}
	return items, ids, urls
}
