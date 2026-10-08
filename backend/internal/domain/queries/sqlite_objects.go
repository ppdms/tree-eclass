package queries

import (
	"context"
	"database/sql"
)

// SQLite object + revision methods over the driver-neutral rdbms.DBTX
// surface. SQL text is copied verbatim from the generated *.sql.go consts;
// the sqlite driver rewrites placeholders/casts at Exec/Query time.
// Timestamps scan via the shared parseTimestamptz helper in sqlite_core.go.

const sqliteDocumentObject = `-- name: DocumentObject :one
SELECT o.id, o.bucket, o.key, o.version_id, o.sha256, o.bytes, o.media_type, o.created_at,r.logical_path,r.course_id FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
WHERE r.document_id=$1 AND r.deleted_at IS NULL
AND (($2::text='' AND EXISTS (
    SELECT 1 FROM knowledge.documents d WHERE d.id=r.document_id AND d.is_current=1 AND d.source_hash=o.sha256
)) OR r.id=$2::text)
ORDER BY r.created_at DESC LIMIT 1
`

func (q *SQLiteQueries) DocumentObject(ctx context.Context, arg DocumentObjectParams) (DocumentObjectRow, error) {
	row := q.db.QueryRow(ctx, sqliteDocumentObject, arg.DocumentID, arg.Revision)
	var i DocumentObjectRow
	var createdAt sql.NullString
	err := row.Scan(
		&i.ID,
		&i.Bucket,
		&i.Key,
		&i.VersionID,
		&i.Sha256,
		&i.Bytes,
		&i.MediaType,
		&createdAt,
		&i.LogicalPath,
		&i.CourseID,
	)
	if err != nil {
		return i, err
	}
	i.CreatedAt = parseTimestamptz(createdAt)
	return i, nil
}

const sqliteFileObject = `-- name: FileObject :one
SELECT o.id, o.bucket, o.key, o.version_id, o.sha256, o.bytes, o.media_type, o.created_at,r.logical_path,r.course_id FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
WHERE r.id=$1 AND r.deleted_at IS NULL AND EXISTS(SELECT 1 FROM app.courses c WHERE c.id=r.course_id AND c.hidden=0)
`

func (q *SQLiteQueries) FileObject(ctx context.Context, id string) (FileObjectRow, error) {
	row := q.db.QueryRow(ctx, sqliteFileObject, id)
	var i FileObjectRow
	var createdAt sql.NullString
	err := row.Scan(
		&i.ID,
		&i.Bucket,
		&i.Key,
		&i.VersionID,
		&i.Sha256,
		&i.Bytes,
		&i.MediaType,
		&createdAt,
		&i.LogicalPath,
		&i.CourseID,
	)
	if err != nil {
		return i, err
	}
	i.CreatedAt = parseTimestamptz(createdAt)
	return i, nil
}

const sqliteRegisterObject = `-- name: RegisterObject :execrows
INSERT INTO app.objects(id,bucket,key,version_id,sha256,bytes,media_type) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(id) DO UPDATE SET id=excluded.id
WHERE app.objects.bucket=excluded.bucket AND app.objects.key=excluded.key
AND app.objects.version_id=excluded.version_id AND app.objects.sha256=excluded.sha256
AND app.objects.bytes=excluded.bytes
`

func (q *SQLiteQueries) RegisterObject(ctx context.Context, arg RegisterObjectParams) (int64, error) {
	result, err := q.db.Exec(ctx, sqliteRegisterObject,
		arg.ID,
		arg.Bucket,
		arg.Key,
		arg.VersionID,
		arg.Sha256,
		arg.Bytes,
		arg.MediaType,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const sqliteRegisterRevision = `-- name: RegisterRevision :exec
INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(document_id,object_id) DO NOTHING
`

func (q *SQLiteQueries) RegisterRevision(ctx context.Context, arg RegisterRevisionParams) error {
	_, err := q.db.Exec(ctx, sqliteRegisterRevision,
		arg.ID,
		arg.DocumentID,
		arg.CourseID,
		arg.LogicalPath,
		arg.ObjectID,
	)
	return err
}
