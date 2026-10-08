// Package objectgc removes abandoned objects only while the controller excludes
// all application writers and holds the dataset operation lock. It is never an
// online worker: a timed grace period cannot protect an in-flight publication.
package objectgc

import (
	"context"
	"fmt"
	"strings"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

type Result struct {
	CatalogRows int64 `json:"catalog_rows_removed"`
	Versions    int64 `json:"object_versions_removed"`
	Bytes       int64 `json:"logical_bytes_removed"`
}

func Collect(ctx context.Context, db *storage.Database, store *blob.Store) (Result, error) {
	var result Result
	predicate, err := unreferenced(ctx, db)
	if err != nil {
		return result, err
	}
	for {
		if err = db.CheckOwner(ctx); err != nil {
			return result, err
		}
		tag, err := db.Pool.Exec(ctx, `DELETE FROM app.objects WHERE id IN
			(SELECT o.id FROM app.objects o WHERE `+predicate+` ORDER BY o.id LIMIT 500)`)
		if err != nil {
			return result, err
		}
		result.CatalogRows += tag.RowsAffected()
		if tag.RowsAffected() == 0 {
			break
		}
	}
	sweep, err := store.PruneUnregistered(ctx, func(ctx context.Context, keys, versions []string) ([]bool, error) {
		if err := db.CheckOwner(ctx); err != nil {
			return nil, err
		}
		keep := make([]bool, 0, len(keys))
		for start := 0; start < len(keys); start += 100 {
			end := start + 100
			if end > len(keys) {
				end = len(keys)
			}
			for _, key := range keys[start:end] {
				var found bool
				if err := db.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.objects o WHERE o.bucket=$1 AND o.key=$2)`, blob.DataBucket, key).Scan(&found); err != nil {
					return nil, err
				}
				keep = append(keep, found)
			}
		}
		return keep, nil
	})
	result.Versions, result.Bytes = sweep.Versions, sweep.Bytes
	return result, err
}

// objectReferences is the static inventory of catalog columns that may hold an
// object id, drawn from the migration chain. It replaces pg_constraint
// discovery so the predicate builds identically on every driver. Historical
// rows count even if their course/document is hidden, removed or no longer
// current.
var objectReferences = []struct{ schema, table, column string }{
	{"app", "document_revisions", "object_id"},
	{"app", "files", "object_id"},
	{"messages", "archive_sources", "object_id"},
	{"messages", "archive_media", "object_id"},
	{"app", "pdf_differences", "old_object_id"},
	{"app", "pdf_differences", "new_object_id"},
	{"app", "pdf_differences", "object_id"},
}

func unreferenced(ctx context.Context, db *storage.Database) (string, error) {
	predicates := []string{
		"o.bucket='tree-eclass-data'",
		"o.key='objects/'||o.sha256",
		"o.id=o.sha256",
		"length(o.id)=64",
	}
	for _, ref := range objectReferences {
		predicates = append(
			predicates,
			fmt.Sprintf(
				"NOT EXISTS (SELECT 1 FROM %s.%s r WHERE r.%s=o.id)",
				ref.schema,
				quoteIdentifier(ref.table),
				quoteIdentifier(ref.column),
			),
		)
	}
	return strings.Join(predicates, " AND "), nil
}

// quoteIdentifier sanitizes a catalog-discovered identifier with manual
// double-quote escaping (replacing the former pgx.Identifier.Sanitize).
func quoteIdentifier(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, "\""+strings.ReplaceAll(part, "\"", "\"\"")+"\"")
	}
	return strings.Join(quoted, ".")
}
