package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

type sqliteObjects struct{ db nativeDBTX }

func (o sqliteObjects) RegisterObject(ctx context.Context, object database.ObjectReference) error {
	result, err := o.db.Exec(ctx, `INSERT INTO objects`+
		`(id,bucket,key,version_id,sha256,bytes,media_type) VALUES(?,?,?,?,?,?,?) `+
		`ON CONFLICT(id) DO UPDATE SET id=excluded.id `+
		`WHERE objects.bucket=excluded.bucket AND objects.key=excluded.key `+
		`AND objects.version_id=excluded.version_id AND objects.sha256=excluded.sha256 `+
		`AND objects.bytes=excluded.bytes`,
		object.SHA256,
		object.Bucket,
		object.Key,
		object.VersionID,
		object.SHA256,
		object.Bytes,
		object.MediaType,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New(
			"object catalog identity differs from storage; " +
				"restore or reconcile the stored revision before publishing",
		)
	}
	return nil
}

func (o sqliteObjects) RegisterRevision(ctx context.Context, params database.RegisterRevisionParams) error {
	_, err := o.db.Exec(ctx, `INSERT INTO document_revisions`+
		`(id,document_id,course_id,logical_path,object_id) VALUES(?,?,?,?,?) `+
		`ON CONFLICT(document_id,object_id) DO NOTHING`,
		params.ID, params.DocumentID, params.CourseID, params.LogicalPath, params.ObjectID)
	return err
}

func (o sqliteObjects) DocumentObject(
	ctx context.Context,
	params database.DocumentObjectParams,
) (database.DocumentObject, error) {
	var out database.DocumentObject
	var id string
	err := o.db.QueryRow(ctx, `SELECT o.id,o.bucket,o.key,o.version_id,o.sha256,o.bytes,`+
		`o.media_type,o.created_at,r.logical_path,r.course_id `+
		`FROM document_revisions r JOIN objects o ON o.id=r.object_id `+
		`WHERE r.document_id=? AND r.deleted_at IS NULL `+
		`AND ((?='' AND EXISTS (`+
		`    SELECT 1 FROM documents d `+
		`    WHERE d.id=r.document_id AND d.is_current=1 AND d.source_hash=o.sha256`+
		`)) OR r.id=?) `+
		`ORDER BY r.created_at DESC LIMIT 1`,
		params.DocumentID, params.Revision, params.Revision).Scan(
		&id,
		&out.Object.Bucket,
		&out.Object.Key,
		&out.Object.VersionID,
		&out.Object.SHA256,
		&out.Object.Bytes,
		&out.Object.MediaType,
		(*nativeTime)(&out.CreatedAt),
		&out.LogicalPath,
		&out.CourseID,
	)
	return out, err
}

func (o sqliteObjects) FileObject(
	ctx context.Context,
	revisionID string,
) (database.DocumentObject, error) {
	var out database.DocumentObject
	var id string
	err := o.db.QueryRow(ctx, `SELECT o.id,o.bucket,o.key,o.version_id,o.sha256,o.bytes,`+
		`o.media_type,o.created_at,r.logical_path,r.course_id `+
		`FROM document_revisions r JOIN objects o ON o.id=r.object_id `+
		`WHERE r.id=? AND r.deleted_at IS NULL `+
		`AND EXISTS(SELECT 1 FROM courses c WHERE c.id=r.course_id AND c.hidden=0)`,
		revisionID).Scan(
		&id,
		&out.Object.Bucket,
		&out.Object.Key,
		&out.Object.VersionID,
		&out.Object.SHA256,
		&out.Object.Bytes,
		&out.Object.MediaType,
		(*nativeTime)(&out.CreatedAt),
		&out.LogicalPath,
		&out.CourseID,
	)
	return out, err
}

func (o sqliteObjects) GetObject(ctx context.Context, id string) (database.ObjectReference, error) {
	var out database.ObjectReference
	err := o.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type `+
		`FROM objects o WHERE o.id=?`, id).Scan(
		&out.Bucket,
		&out.Key,
		&out.VersionID,
		&out.SHA256,
		&out.Bytes,
		&out.MediaType,
	)
	return out, err
}
