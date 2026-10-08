package rdbms

import (
	"context"
	"strings"
)

// postgresObjectCatalog prunes objects no catalog column references. The
// discovery query unions the statically declared reference columns with every
// live foreign key pointing at the objects catalog, so references added by
// future migrations stay preserved without a code change.
type postgresObjectCatalog struct{ db nativeDBTX }

// objectCatalogKnownReferences is the static inventory of catalog columns that
// may hold an object id, drawn from the migration chain. Discovery unions it
// with live foreign keys so identity-only mirrors added later remain safe.
var objectCatalogKnownReferences = []objectCatalogReference{
	{"app", "document_revisions", "object_id"},
	{"app", "files", "object_id"},
	{"messages", "archive_sources", "object_id"},
	{"messages", "archive_media", "object_id"},
	{"app", "pdf_differences", "old_object_id"},
	{"app", "pdf_differences", "new_object_id"},
	{"app", "pdf_differences", "object_id"},
}

func (r postgresObjectCatalog) DeleteUnreferencedObjects(ctx context.Context, bucket string, batch int) (int64, error) {
	references, err := postgresObjectReferences(ctx, r.db)
	if err != nil {
		return 0, err
	}
	var conditions strings.Builder
	for _, ref := range references {
		conditions.WriteString(" AND NOT EXISTS (SELECT 1 FROM ")
		conditions.WriteString(quoteObjectCatalogIdentifier(ref.schema))
		conditions.WriteString(".")
		conditions.WriteString(quoteObjectCatalogIdentifier(ref.table))
		conditions.WriteString(" r WHERE r.")
		conditions.WriteString(quoteObjectCatalogIdentifier(ref.column))
		conditions.WriteString("=o.id)")
	}
	result, err := r.db.Exec(ctx, `DELETE FROM app.objects WHERE id IN
		(SELECT o.id FROM app.objects o
		WHERE o.bucket=$1 AND o.key='objects/'||o.sha256 AND o.id=o.sha256
		AND length(o.id)=64`+conditions.String()+`
		ORDER BY o.id LIMIT $2)`, bucket, batch)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (r postgresObjectCatalog) CatalogHasKeys(ctx context.Context, bucket string, keys []string) ([]bool, error) {
	keep := make([]bool, 0, len(keys))
	for start := 0; start < len(keys); start += 100 {
		end := start + 100
		if end > len(keys) {
			end = len(keys)
		}
		for _, key := range keys[start:end] {
			var found bool
			if err := r.db.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM app.objects o WHERE o.bucket=$1 AND o.key=$2)`,
				bucket, key).Scan(&found); err != nil {
				return nil, err
			}
			keep = append(keep, found)
		}
	}
	return keep, nil
}

type objectCatalogReference struct{ schema, table, column string }

// postgresObjectReferences unions the static reference inventory with every
// single-column foreign key whose target is the objects catalog id. pg_catalog
// supplies the live constraint map; the static inventory carries the declared
// catalog shape so pruning never depends on constraint introspection alone.
func postgresObjectReferences(ctx context.Context, db nativeDBTX) ([]objectCatalogReference, error) {
	seen := make(map[objectCatalogReference]bool, len(objectCatalogKnownReferences))
	references := append([]objectCatalogReference(nil), objectCatalogKnownReferences...)
	for _, ref := range references {
		seen[ref] = true
	}
	rows, err := db.Query(ctx, `
		SELECT n.nspname, c.relname, a.attname
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class c ON c.oid=con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		JOIN pg_catalog.pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=con.conkey[1]
		JOIN pg_catalog.pg_attribute t ON t.attrelid=con.confrelid AND t.attnum=con.confkey[1]
		WHERE con.contype='f' AND con.confrelid='app.objects'::regclass
		AND array_length(con.conkey,1)=1 AND t.attname='id'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref objectCatalogReference
		if err := rows.Scan(&ref.schema, &ref.table, &ref.column); err != nil {
			return nil, err
		}
		if ref.schema == "" || ref.table == "" || ref.column == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		references = append(references, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return references, nil
}

// quoteObjectCatalogIdentifier sanitizes a catalog-discovered identifier with
// manual double-quote escaping.
func quoteObjectCatalogIdentifier(part string) string {
	return `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
}
