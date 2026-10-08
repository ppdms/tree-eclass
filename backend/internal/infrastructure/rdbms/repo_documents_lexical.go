package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// Serialized PostgreSQL tsvectors contain positions, not just searchable text;
// SQLite lower() also cannot provide the application's Unicode case folding.
// Stream the ordered native rows through one canonical matcher. The result cap
// applies after matching, and corpus growth does not grow retained memory.
func lexicalCandidates(ctx context.Context, db nativeDBTX, query string, args []any,
	terms []string, limit int) ([]database.SearchCandidate, error) {
	out := []database.SearchCandidate{}
	if limit <= 0 || len(terms) == 0 {
		return out, nil
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		row, err := scanSearchCandidate(rows, false)
		if err != nil {
			return nil, err
		}
		if matchesLexical(row.SearchCandidate, terms) {
			out = append(out, row.SearchCandidate)
			if len(out) == limit {
				return out, nil
			}
		}
	}
	return out, rows.Err()
}

func matchesLexical(row database.SearchCandidate, terms []string) bool {
	heading := ""
	if row.Heading != nil {
		heading = *row.Heading
	}
	return matchesLexicalText(strings.Join([]string{
		row.Text, heading, row.Document.DisplayName, row.Document.SourcePath, row.Document.CourseName,
	}, " "), terms)
}

func matchesLexicalText(text string, terms []string) bool {
	text = identity.Search(identity.Decode(text))
	for _, term := range terms {
		if !strings.Contains(text, term) {
			return false
		}
	}
	return true
}
