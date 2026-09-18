package synchronization

import (
	"context"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/storage"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func publishDocument(ctx context.Context, tx pgx.Tx, course queries.AppCourse, file File) (string, error) {
	q := queries.New(tx)
	o := file.Object
	document := identity.Document(course.ID, file.Path)
	revision := identity.Stable("rev", document, o.SHA256)
	if err := q.QueueLock(ctx, "document:"+document); err != nil {
		return "", err
	}
	if err := storage.RegisterObject(ctx, tx, *o); err != nil {
		return "", err
	}
	if err := q.RegisterRevision(
		ctx,
		queries.RegisterRevisionParams{
			ID:          revision,
			DocumentID:  document,
			CourseID:    course.ID,
			LogicalPath: file.Path,
			ObjectID:    o.SHA256,
		},
	); err != nil {
		return "", err
	}
	kind := materials.Kind(file.Path, o.MediaType)
	status := "pending"
	if kind == "" {
		kind = "unsupported"
		status = "unsupported"
	}
	err := tx.QueryRow(ctx, `INSERT INTO knowledge.documents(id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,source_hash,source_url,source_etag,mime_type,document_kind,source_size_bytes,status,source_origin,content_hash_verified,source_modified_at)
VALUES($1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$10,$11,$12,$13,'eclass',1,$14)
ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,course_short_name=excluded.course_short_name,display_name=excluded.display_name,
source_hash=excluded.source_hash,source_url=excluded.source_url,source_etag=excluded.source_etag,mime_type=excluded.mime_type,document_kind=excluded.document_kind,source_size_bytes=excluded.source_size_bytes,
source_modified_at=excluded.source_modified_at,is_current=1,content_hash_verified=1,
status=CASE WHEN knowledge.documents.source_hash<>excluded.source_hash OR knowledge.documents.is_current=0 THEN excluded.status ELSE knowledge.documents.status END
WHERE knowledge.documents.source_origin='eclass' RETURNING status`,
		document,
		course.ID,
		course.Name,
		course.ShortName,
		identity.Encode(file.Path),
		identity.Encode(file.Name),
		o.SHA256,
		file.URL,
		file.ETag,
		o.MediaType,
		kind,
		o.Bytes,
		status,
		file.Updated,
	).
		Scan(&status)
	if err != nil {
		return "", err
	}
	if status == "pending" {
		if _, err = jobs.EnqueueTx(ctx, tx, "index", "index_document", map[string]string{"document_id": document}, false); err != nil {
			return "", err
		}
	}
	return revision, nil
}
