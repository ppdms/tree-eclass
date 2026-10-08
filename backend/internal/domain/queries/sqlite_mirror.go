package queries

import (
	"context"
)

// SQLite external-mirror method over the driver-neutral rdbms.DBTX surface.
// SQL text is copied verbatim from the generated *.sql.go const; the sqlite
// driver rewrites placeholders/casts at Exec/Query time.

const sqliteExternalMirrorFiles = `-- name: ExternalMirrorFiles :many
SELECT d.normalized_path,o.bucket,o.key,o.version_id,o.sha256,o.bytes
FROM knowledge.documents d
JOIN app.document_revisions r
  ON r.document_id=d.id
 AND r.course_id=d.course_id
 AND r.logical_path=d.normalized_path
 AND r.deleted_at IS NULL
JOIN app.objects o ON o.id=r.object_id AND o.sha256=d.source_hash
WHERE d.course_id=$1
  AND d.source_origin='external'
  AND d.is_current=1
  AND NOT EXISTS (
      SELECT 1 FROM knowledge.archive_members m WHERE m.child_document_id=d.id
  )
ORDER BY d.normalized_path
`

func (q *SQLiteQueries) ExternalMirrorFiles(ctx context.Context, courseID int64) ([]ExternalMirrorFilesRow, error) {
	rows, err := q.db.Query(ctx, sqliteExternalMirrorFiles, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ExternalMirrorFilesRow{}
	for rows.Next() {
		var i ExternalMirrorFilesRow
		if err := rows.Scan(
			&i.NormalizedPath,
			&i.Bucket,
			&i.Key,
			&i.VersionID,
			&i.Sha256,
			&i.Bytes,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
