package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// Unicode case folding and exact timestamp comparison are value-level contract
// rules, not SQL-dialect shims. Each adapter supplies its own ordered native query.
func listAdminDocumentRows(ctx context.Context, db nativeDBTX, statement string, args []any,
	query string, limit int) ([]database.AdminDocument, error) {
	rows, err := db.Query(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.AdminDocument{}
	query = strings.ToLower(query)
	for len(out) < limit && rows.Next() {
		var item database.AdminDocument
		pointers := append(scanDocumentPointers(&item.KnowledgeDocument), &item.ChunkCount, &item.EmbeddingCount)
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		if query != "" && !strings.Contains(strings.ToLower(identity.Decode(item.DisplayName)), query) &&
			!strings.Contains(strings.ToLower(identity.Decode(item.SourcePath)), query) {
			continue
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func listMaterialRows(ctx context.Context, db nativeDBTX, statement string, args []any,
	since *string, limit int) ([]database.KnowledgeDocument, error) {
	var cutoff nativeTime
	if since != nil {
		if err := cutoff.Scan(*since); err != nil {
			return nil, err
		}
	}
	rows, err := db.Query(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.KnowledgeDocument{}
	for len(out) <= limit && rows.Next() {
		var doc database.KnowledgeDocument
		if err := rows.Scan(scanDocumentPointers(&doc)...); err != nil {
			return nil, err
		}
		admitted, err := materialAfter(doc.IndexedAt, cutoff)
		if err != nil {
			return nil, err
		}
		if admitted {
			out = append(out, doc)
		}
	}
	return out, rows.Err()
}

func materialAfter(indexed *string, cutoff nativeTime) (bool, error) {
	if !cutoff.Valid {
		return true, nil
	}
	if indexed == nil {
		return false, nil
	}
	var instant nativeTime
	if err := instant.Scan(*indexed); err != nil {
		return false, err
	}
	return !instant.Time.Before(cutoff.Time), nil
}
