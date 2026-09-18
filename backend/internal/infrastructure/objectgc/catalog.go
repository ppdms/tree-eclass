// Package objectgc removes abandoned objects only while the controller excludes
// all application writers and holds the dataset operation lock. It is never an
// online worker: a timed grace period cannot protect an in-flight publication.
package objectgc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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
		rows, err := db.Pool.Query(ctx, `SELECT EXISTS(SELECT FROM app.objects o
			WHERE o.bucket=$1 AND o.key=u.key AND o.version_id=u.version)
			FROM unnest($2::text[], $3::text[]) WITH ORDINALITY u(key,version,n) ORDER BY n`, blob.DataBucket, keys, versions)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		keep := make([]bool, 0, len(keys))
		for rows.Next() {
			var found bool
			if err = rows.Scan(&found); err != nil {
				return nil, err
			}
			keep = append(keep, found)
		}
		return keep, rows.Err()
	})
	result.Versions, result.Bytes = sweep.Versions, sweep.Bytes
	return result, err
}

// Discover every actual foreign key, including future domains. Historical rows
// count even if their course/document is hidden, removed or no longer current.
func unreferenced(ctx context.Context, db *storage.Database) (string, error) {
	rows, err := db.Pool.Query(ctx, `SELECT n.nspname,t.relname,a.attname,
		cardinality(c.conkey), cardinality(c.confkey), target.attname
		FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=c.conkey[1]
		JOIN pg_attribute target ON target.attrelid=c.confrelid AND target.attnum=c.confkey[1]
		WHERE c.contype='f' AND c.confrelid='app.objects'::regclass ORDER BY c.oid`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	predicates := []string{
		"o.bucket='tree-eclass-data'",
		"o.key='objects/'||o.sha256",
		"o.id=o.sha256",
		"o.id~'^[0-9a-f]{64}$'",
	}
	references := 0
	for rows.Next() {
		var schema, table, column, target string
		var fromCount, toCount int
		if err = rows.Scan(&schema, &table, &column, &fromCount, &toCount, &target); err != nil {
			return "", err
		}
		if fromCount != 1 || toCount != 1 || target != "id" {
			return "", errors.New("unsupported object reference; garbage collection refused")
		}
		references++
		predicates = append(
			predicates,
			fmt.Sprintf(
				"NOT EXISTS (SELECT FROM %s r WHERE r.%s=o.id)",
				pgx.Identifier{schema, table}.Sanitize(),
				pgx.Identifier{column}.Sanitize(),
			),
		)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if references == 0 {
		return "", errors.New("object reference constraints are missing; garbage collection refused")
	}
	return strings.Join(predicates, " AND "), nil
}
