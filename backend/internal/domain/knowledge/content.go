package knowledge

import (
	"context"
	"path"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/blob"
)

type Content struct {
	Object blob.Reference
	Name   string
}

// Content admits the exact immutable version and current archive ancestry in
// one SQL snapshot. An explicit historic revision never authorizes a different
// document or course, and pending/stale current sources cannot become readers.
func (s Reader) Content(ctx context.Context, course int64, document, revision string) (Content, error) {
	result := Content{}
	err := s.Pool.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type,d.display_name
 FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id
 JOIN app.document_revisions r ON r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path AND r.deleted_at IS NULL
 JOIN app.objects o ON o.id=r.object_id
 WHERE d.id=$1 AND d.course_id=$2 AND d.status='ready' AND `+CurrentSourcePredicate+`
 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1))
 AND (($3='' AND o.sha256=d.source_hash) OR ($3<>'' AND r.id=$3)) ORDER BY r.created_at DESC LIMIT 1`,
		document,
		course,
		revision,
	).Scan(
		&result.Object.Bucket,
		&result.Object.Key,
		&result.Object.VersionID,
		&result.Object.SHA256,
		&result.Object.Bytes,
		&result.Object.MediaType,
		&result.Name,
	)
	result.Name = identity.Decode(result.Name)
	return result, err
}

// LogicalContent preserves explicit historical file-version paths while current
// compatibility paths obey the same source admission as the id-based reader.
func (s Reader) LogicalContent(ctx context.Context, logical string) (Content, error) {
	result := Content{}
	var name string
	err := s.Pool.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type,r.logical_path
 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id JOIN app.courses c ON c.id=r.course_id
 WHERE r.deleted_at IS NULL AND c.hidden=0 AND (
 (r.logical_path=$1 AND EXISTS(SELECT 1 FROM knowledge.documents d WHERE d.id=r.document_id AND d.course_id=r.course_id AND d.source_hash=o.sha256 AND `+CurrentSourcePredicate+`))
 OR EXISTS(SELECT 1 FROM app.file_versions v WHERE v.revision_id=r.id AND v.version_webdav_path=$1))
 ORDER BY r.created_at DESC LIMIT 1`,
		logical,
	).Scan(
		&result.Object.Bucket,
		&result.Object.Key,
		&result.Object.VersionID,
		&result.Object.SHA256,
		&result.Object.Bytes,
		&result.Object.MediaType,
		&name,
	)
	result.Name = path.Base(identity.Decode(name))
	return result, err
}
