-- name: ObserveDocument :exec
INSERT INTO knowledge.documents(
    id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,
    source_hash,mime_type,document_kind,source_size_bytes,status,source_origin,content_hash_verified
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending','external',1);

-- name: MaterialMetadata :exec
INSERT INTO app.external_material_metadata(course_id,source_path,material_type,source_label,notes)
VALUES($1,$2,$3,'Browser upload','')
ON CONFLICT(course_id,source_path) DO UPDATE SET material_type=excluded.material_type;

-- name: ExternalMaterials :many
SELECT d.*,m.material_type,m.source_label
FROM knowledge.documents d
LEFT JOIN app.external_material_metadata m ON m.course_id=d.course_id AND m.source_path=d.source_path
WHERE d.course_id=$1 AND d.source_origin='external' AND d.is_current=1 ORDER BY d.display_name,d.id;

-- name: Material :one
SELECT d.*,m.material_type,m.source_label
FROM knowledge.documents d
LEFT JOIN app.external_material_metadata m ON m.course_id=d.course_id AND m.source_path=d.source_path
WHERE d.course_id=$1 AND d.id=$2 AND d.source_origin='external' AND d.is_current=1;

-- name: IndexDocument :one
SELECT * FROM knowledge.documents WHERE id=$1 AND is_current=1;

-- name: ReplaceChunks :exec
DELETE FROM knowledge.chunks WHERE document_id=$1;

-- name: InsertChunk :exec
INSERT INTO knowledge.chunks(
    id,document_id,ordinal,locator_type,locator_start,locator_end,heading,text,
    normalized_text,content_hash,metadata_json
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);

-- name: IndexChunkSearch :exec
INSERT INTO knowledge.chunks_fts(chunk_id,text,normalized_text,heading,display_name,source_path,course_name)
VALUES($1,$2,$3,$4,$5,$6,$7);

-- name: MarkIndexed :exec
UPDATE knowledge.documents
SET status='ready',page_count=$2,character_count=$3,word_count=$4,reading_minutes=$5,
    extractor_name='tree-parser',extractor_version='1',indexed_at=$6,error=NULL,warnings_json=$7,
    complexity_score=$8,complexity_label=$9
WHERE id=$1;
