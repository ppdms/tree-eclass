package rdbms

import (
	"context"
	"strings"
)

// sqliteObjectCatalog prunes objects no catalog column references. SQLite has
// no schemas; discovery unions the declared reference columns with every live
// foreign key pointing at the objects catalog id, so references added by
// future migrations stay preserved without a code change.
type sqliteObjectCatalog struct{ db nativeDBTX }

// sqliteObjectCatalogKnownReferences mirrors the postgres reference inventory
// without schema qualifiers.
var sqliteObjectCatalogKnownReferences = []struct{ table, column string }{
	{"document_revisions", "object_id"},
	{"files", "object_id"},
	{"archive_sources", "object_id"},
	{"archive_media", "object_id"},
	{"pdf_differences", "old_object_id"},
	{"pdf_differences", "new_object_id"},
	{"pdf_differences", "object_id"},
}

func (r sqliteObjectCatalog) DeleteUnreferencedObjects(ctx context.Context, bucket string, batch int) (int64, error) {
	references, err := sqliteObjectReferences(ctx, r.db)
	if err != nil {
		return 0, err
	}
	var conditions strings.Builder
	for _, ref := range references {
		conditions.WriteString(" AND NOT EXISTS (SELECT 1 FROM ")
		conditions.WriteString(quoteObjectCatalogIdentifier(ref.table))
		conditions.WriteString(" r WHERE r.")
		conditions.WriteString(quoteObjectCatalogIdentifier(ref.column))
		conditions.WriteString("=o.id)")
	}
	result, err := r.db.Exec(ctx, `DELETE FROM objects WHERE id IN
		(SELECT o.id FROM objects o
		WHERE o.bucket=? AND o.key='objects/'||o.sha256 AND o.id=o.sha256
		AND length(o.id)=64`+conditions.String()+`
		ORDER BY o.id LIMIT ?)`, bucket, batch)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (r sqliteObjectCatalog) CatalogHasKeys(ctx context.Context, bucket string, keys []string) ([]bool, error) {
	keep := make([]bool, 0, len(keys))
	for start := 0; start < len(keys); start += 100 {
		end := start + 100
		if end > len(keys) {
			end = len(keys)
		}
		for _, key := range keys[start:end] {
			var found bool
			if err := r.db.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM objects o WHERE o.bucket=? AND o.key=?)`,
				bucket, key).Scan(&found); err != nil {
				return nil, err
			}
			keep = append(keep, found)
		}
	}
	return keep, nil
}

type sqliteObjectCatalogReference struct{ table, column string }

// sqliteObjectReferences unions the declared reference inventory with every
// live foreign key whose target is the objects catalog id. sqlite_master
// enumerates user tables; PRAGMA foreign_key_list exposes each table's keys.
func sqliteObjectReferences(ctx context.Context, db nativeDBTX) ([]sqliteObjectCatalogReference, error) {
	seen := make(map[sqliteObjectCatalogReference]bool, len(sqliteObjectCatalogKnownReferences))
	references := make([]sqliteObjectCatalogReference, 0, len(sqliteObjectCatalogKnownReferences))
	for _, ref := range sqliteObjectCatalogKnownReferences {
		entry := sqliteObjectCatalogReference{table: ref.table, column: ref.column}
		seen[entry] = true
		references = append(references, entry)
	}
	tables, err := sqliteUserTables(ctx, db)
	if err != nil {
		return nil, err
	}
	for _, table := range tables {
		columns, err := sqliteObjectForeignColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, column := range columns {
			entry := sqliteObjectCatalogReference{table: table, column: column}
			if seen[entry] {
				continue
			}
			seen[entry] = true
			references = append(references, entry)
		}
	}
	return references, nil
}

func sqliteUserTables(ctx context.Context, db nativeDBTX) ([]string, error) {
	rows, err := db.Query(ctx, `SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' AND name<>'objects'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			tables = append(tables, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// sqliteObjectForeignColumns returns the columns of table whose foreign key
// target is the objects catalog id.
func sqliteObjectForeignColumns(ctx context.Context, db nativeDBTX, table string) ([]string, error) {
	rows, err := db.Query(ctx, `PRAGMA foreign_key_list(`+quoteObjectCatalogIdentifier(table)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var id, seq int64
		var target, source, destination, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &target, &source, &destination, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		if target == "objects" && destination == "id" && source != "" {
			columns = append(columns, source)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}
