package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

type postgresObjects struct{ db nativeDBTX }

func (o postgresObjects) RegisterObject(ctx context.Context, object database.ObjectReference) error {
	result, err := o.db.Exec(ctx, `INSERT INTO app.objects`+
		`(id,bucket,key,version_id,sha256,bytes,media_type) VALUES($1,$2,$3,$4,$5,$6,$7) `+
		`ON CONFLICT(id) DO UPDATE SET id=excluded.id `+
		`WHERE app.objects.bucket=excluded.bucket AND app.objects.key=excluded.key `+
		`AND app.objects.version_id=excluded.version_id AND app.objects.sha256=excluded.sha256 `+
		`AND app.objects.bytes=excluded.bytes`,
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

func (o postgresObjects) RegisterRevision(ctx context.Context, params database.RegisterRevisionParams) error {
	_, err := o.db.Exec(ctx, `INSERT INTO app.document_revisions`+
		`(id,document_id,course_id,logical_path,object_id) VALUES($1,$2,$3,$4,$5) `+
		`ON CONFLICT(document_id,object_id) DO NOTHING`,
		params.ID, params.DocumentID, params.CourseID, params.LogicalPath, params.ObjectID)
	return err
}

func (o postgresObjects) DocumentObject(
	ctx context.Context,
	params database.DocumentObjectParams,
) (database.DocumentObject, error) {
	// IDs are content digests (id == sha256); the object id column is not
	// selected separately because callers use SHA256 as the identity.
	var out database.DocumentObject
	var id string
	err := o.db.QueryRow(ctx, `SELECT o.id,o.bucket,o.key,o.version_id,o.sha256,o.bytes,`+
		`o.media_type,o.created_at,r.logical_path,r.course_id `+
		`FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id `+
		`WHERE r.document_id=$1 AND r.deleted_at IS NULL `+
		`AND (($2::text='' AND EXISTS (`+
		`    SELECT 1 FROM knowledge.documents d `+
		`    WHERE d.id=r.document_id AND d.is_current=1 AND d.source_hash=o.sha256`+
		`)) OR r.id=$2::text) `+
		`ORDER BY r.created_at DESC LIMIT 1`,
		params.DocumentID, params.Revision).Scan(
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

func (o postgresObjects) FileObject(
	ctx context.Context,
	revisionID string,
) (database.DocumentObject, error) {
	var out database.DocumentObject
	var id string
	err := o.db.QueryRow(ctx, `SELECT o.id,o.bucket,o.key,o.version_id,o.sha256,o.bytes,`+
		`o.media_type,o.created_at,r.logical_path,r.course_id `+
		`FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id `+
		`WHERE r.id=$1 AND r.deleted_at IS NULL `+
		`AND EXISTS(SELECT 1 FROM app.courses c WHERE c.id=r.course_id AND c.hidden=0)`,
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

func (o postgresObjects) GetObject(ctx context.Context, id string) (database.ObjectReference, error) {
	var out database.ObjectReference
	err := o.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type `+
		`FROM app.objects o WHERE o.id=$1`, id).Scan(
		&out.Bucket,
		&out.Key,
		&out.VersionID,
		&out.SHA256,
		&out.Bytes,
		&out.MediaType,
	)
	return out, err
}
