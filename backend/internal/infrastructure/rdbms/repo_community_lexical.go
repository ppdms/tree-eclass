package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func communityLexicalCandidates(ctx context.Context, db nativeDBTX, query string, args []any,
	terms []string, limit int) ([]database.CommunityCandidate, error) {
	out := []database.CommunityCandidate{}
	if len(args) == 0 || len(terms) == 0 || limit <= 0 {
		return out, nil
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var text string
		item, err := scanCommunityCandidate(rows, &text)
		if err != nil {
			return nil, err
		}
		if matchesLexicalText(text+" "+item.ChannelName, terms) {
			out = append(out, item)
			if len(out) == limit {
				return out, nil
			}
		}
	}
	return out, rows.Err()
}
