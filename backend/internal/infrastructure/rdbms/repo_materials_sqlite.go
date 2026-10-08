package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteMaterials struct{ db nativeDBTX }

const sqliteMaterialColumns = `d.id,d.course_id,d.course_name,d.course_short_name,d.source_path,` +
	`d.source_origin,d.normalized_path,d.source_url,d.display_name,d.source_hash,` +
	`d.source_fingerprint,d.source_etag,d.content_hash_verified,d.mime_type,` +
	`d.response_mime_type,d.document_kind,d.academic_year,d.source_modified_at,` +
	`d.is_current,d.status,d.page_count,d.source_size_bytes,d.character_count,` +
	`d.word_count,d.reading_minutes,d.complexity_score,d.complexity_label,` +
	`d.language_hint,d.extractor_name,d.extractor_version,d.indexed_at,d.error,` +
	`d.diagnostic_reason,d.warnings_json,m.material_type,m.source_label`

func scanSQLiteMaterialRow(rows nativeRows) (database.MaterialRow, error) {
	var row database.MaterialRow
	doc := &row.KnowledgeDocument
	err := rows.Scan(
		&doc.ID,
		&doc.CourseID,
		&doc.CourseName,
		&doc.CourseShortName,
		&doc.SourcePath,
		&doc.SourceOrigin,
		&doc.NormalizedPath,
		&doc.SourceUrl,
		&doc.DisplayName,
		&doc.SourceHash,
		&doc.SourceFingerprint,
		&doc.SourceEtag,
		&doc.ContentHashVerified,
		&doc.MimeType,
		&doc.ResponseMimeType,
		&doc.DocumentKind,
		&doc.AcademicYear,
		&doc.SourceModifiedAt,
		&doc.IsCurrent,
		&doc.Status,
		&doc.PageCount,
		&doc.SourceSizeBytes,
		&doc.CharacterCount,
		&doc.WordCount,
		&doc.ReadingMinutes,
		&doc.ComplexityScore,
		&doc.ComplexityLabel,
		&doc.LanguageHint,
		&doc.ExtractorName,
		&doc.ExtractorVersion,
		&doc.IndexedAt,
		&doc.Error,
		&doc.DiagnosticReason,
		&doc.WarningsJson,
		&row.MaterialType,
		&row.SourceLabel,
	)
	return row, err
}

func (m sqliteMaterials) GetMaterial(
	ctx context.Context,
	courseID int64,
	id string,
) (database.MaterialRow, error) {
	var row database.MaterialRow
	doc := &row.KnowledgeDocument
	err := m.db.QueryRow(ctx, `SELECT `+sqliteMaterialColumns+`
FROM documents d
LEFT JOIN external_material_metadata m
  ON m.course_id=d.course_id AND m.source_path=d.source_path
WHERE d.course_id=? AND d.id=? AND d.source_origin='external' AND d.is_current=1`,
		courseID, id).Scan(
		&doc.ID,
		&doc.CourseID,
		&doc.CourseName,
		&doc.CourseShortName,
		&doc.SourcePath,
		&doc.SourceOrigin,
		&doc.NormalizedPath,
		&doc.SourceUrl,
		&doc.DisplayName,
		&doc.SourceHash,
		&doc.SourceFingerprint,
		&doc.SourceEtag,
		&doc.ContentHashVerified,
		&doc.MimeType,
		&doc.ResponseMimeType,
		&doc.DocumentKind,
		&doc.AcademicYear,
		&doc.SourceModifiedAt,
		&doc.IsCurrent,
		&doc.Status,
		&doc.PageCount,
		&doc.SourceSizeBytes,
		&doc.CharacterCount,
		&doc.WordCount,
		&doc.ReadingMinutes,
		&doc.ComplexityScore,
		&doc.ComplexityLabel,
		&doc.LanguageHint,
		&doc.ExtractorName,
		&doc.ExtractorVersion,
		&doc.IndexedAt,
		&doc.Error,
		&doc.DiagnosticReason,
		&doc.WarningsJson,
		&row.MaterialType,
		&row.SourceLabel,
	)
	return row, err
}

func (m sqliteMaterials) ListMaterials(
	ctx context.Context,
	courseID int64,
) ([]database.MaterialRow, error) {
	rows, err := m.db.Query(ctx, `SELECT `+sqliteMaterialColumns+`
FROM documents d
LEFT JOIN external_material_metadata m
  ON m.course_id=d.course_id AND m.source_path=d.source_path
WHERE d.course_id=? AND d.source_origin='external' AND d.is_current=1
ORDER BY d.display_name,d.id`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.MaterialRow{}
	for rows.Next() {
		row, err := scanSQLiteMaterialRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m sqliteMaterials) SetMaterialMetadata(
	ctx context.Context,
	params database.MaterialMetadataParams,
) error {
	_, err := m.db.Exec(ctx, `INSERT INTO external_material_metadata`+
		`(course_id,source_path,material_type,source_label,notes) `+
		`VALUES(?,?,?,'Browser upload','') `+
		`ON CONFLICT(course_id,source_path) DO UPDATE SET material_type=excluded.material_type`,
		params.CourseID, params.SourcePath, params.MaterialType)
	return err
}

func (m sqliteMaterials) ExternalMirrorFiles(
	ctx context.Context,
	courseID int64,
) ([]database.MirrorFile, error) {
	rows, err := m.db.Query(ctx, `SELECT d.normalized_path,o.bucket,o.key,o.version_id,o.sha256,o.bytes
FROM documents d
JOIN document_revisions r
  ON r.document_id=d.id
 AND r.course_id=d.course_id
 AND r.logical_path=d.normalized_path
 AND r.deleted_at IS NULL
JOIN objects o ON o.id=r.object_id AND o.sha256=d.source_hash
WHERE d.course_id=?
  AND d.source_origin='external'
  AND d.is_current=1
  AND NOT EXISTS (
      SELECT 1 FROM archive_members m WHERE m.child_document_id=d.id
  )
ORDER BY d.normalized_path`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.MirrorFile{}
	for rows.Next() {
		var file database.MirrorFile
		if err := rows.Scan(
			&file.NormalizedPath,
			&file.Bucket,
			&file.Key,
			&file.VersionID,
			&file.SHA256,
			&file.Bytes,
		); err != nil {
			return nil, err
		}
		out = append(out, file)
	}
	return out, rows.Err()
}

func (m sqliteMaterials) ListPresentation(
	ctx context.Context,
	params database.MaterialPresentationParams,
) ([]database.MaterialPresentationRow, error) {
	rows, err := m.db.Query(ctx, `SELECT d.id,d.course_id,d.display_name,d.source_path,`+
		`d.source_origin,d.document_kind,d.status,d.diagnostic_reason,d.source_modified_at,`+
		`d.indexed_at,d.source_size_bytes,d.page_count,d.reading_minutes,`+
		`m.material_type,m.source_label,
CASE WHEN d.status='ready' AND e.status='ready' AND e.source_hash=d.source_hash
  AND coalesce(e.requested_model,e.model)=?
  AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END
  AND e.payload_json IS NOT NULL AND json_valid(e.payload_json)
  AND json_type(e.payload_json)='object'
THEN json_object('external_material_type',json_extract(e.payload_json,'$.external_material_type'),
  'material_type',json_extract(e.payload_json,'$.material_type'))
ELSE '{}' END
FROM documents d
LEFT JOIN external_material_metadata m
  ON m.course_id=d.course_id AND m.source_path=d.normalized_path
LEFT JOIN document_enrichments e ON e.document_id=d.id
WHERE d.course_id=? AND d.is_current=1 AND d.source_origin='external'
  AND (?='' OR d.id=?)
ORDER BY d.display_name,d.id`,
		params.Model,
		params.PageVersion,
		params.DocumentVersion,
		params.CourseID,
		params.DocumentID,
		params.DocumentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.MaterialPresentationRow{}
	for rows.Next() {
		var row database.MaterialPresentationRow
		if err := rows.Scan(
			&row.ID,
			&row.CourseID,
			&row.DisplayName,
			&row.SourcePath,
			&row.SourceOrigin,
			&row.DocumentKind,
			&row.Status,
			&row.DiagnosticReason,
			&row.SourceModifiedAt,
			&row.IndexedAt,
			&row.SourceSizeBytes,
			&row.PageCount,
			&row.ReadingMinutes,
			&row.MaterialType,
			&row.SourceLabel,
			&row.EnrichmentPayload,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
