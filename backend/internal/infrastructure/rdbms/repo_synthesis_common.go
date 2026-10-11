package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type sqliteSynthesis struct{ db nativeDBTX }

func synthesisTable(lane database.SynthesisLane) string {
	if lane == database.SynthesisPractice {
		return "practice_question_sets"
	}
	return "course_blueprints"
}

// Match decoded source text, never backend-specific full-text index serialization.
func synthesisCommunityIDs(ctx context.Context, db nativeDBTX, query string,
	params database.SynthesisCommunityParams) ([]string, error) {
	out := []string{}
	if len(params.Terms) == 0 || params.Limit <= 0 {
		return out, nil
	}
	terms := make([]string, len(params.Terms))
	for i, term := range params.Terms {
		terms[i] = identity.Search(identity.Decode(term))
	}
	rows, err := db.Query(ctx, query, params.CourseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, text, channel string
		if err := rows.Scan(&id, &text, &channel); err != nil {
			return nil, err
		}
		if matchesLexicalText(text+" "+channel, terms) {
			out = append(out, id)
			if int64(len(out)) == params.Limit {
				return out, nil
			}
		}
	}
	return out, rows.Err()
}
