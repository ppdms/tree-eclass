-- name: RegisterObject :execrows
INSERT INTO app.objects(id,bucket,key,version_id,sha256,bytes,media_type) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(id) DO UPDATE SET id=excluded.id
WHERE app.objects.bucket=excluded.bucket AND app.objects.key=excluded.key
AND app.objects.version_id=excluded.version_id AND app.objects.sha256=excluded.sha256
AND app.objects.bytes=excluded.bytes;

-- name: RegisterRevision :exec
INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(document_id,object_id) DO NOTHING;

-- name: DocumentObject :one
SELECT o.*,r.logical_path,r.course_id FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
WHERE r.document_id=$1 AND r.deleted_at IS NULL
AND ((sqlc.arg(revision)::text='' AND EXISTS (
    SELECT 1 FROM knowledge.documents d WHERE d.id=r.document_id AND d.is_current=1 AND d.source_hash=o.sha256
)) OR r.id=sqlc.arg(revision)::text)
ORDER BY r.created_at DESC LIMIT 1;

-- name: FileObject :one
SELECT o.*,r.logical_path,r.course_id FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
WHERE r.id=$1 AND r.deleted_at IS NULL AND EXISTS(SELECT 1 FROM app.courses c WHERE c.id=r.course_id AND c.hidden=0);
