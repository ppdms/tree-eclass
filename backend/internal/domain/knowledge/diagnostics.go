package knowledge

import (
	"context"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Coverage struct {
	CourseID           int64 `json:"course_id"`
	SupportedDocuments int64 `json:"supported_documents"`
	IndexedDocuments   int64 `json:"indexed_documents"`
	FailedDocuments    int64 `json:"failed_documents"`
	PendingDocuments   int64 `json:"pending_documents"`
}

func (s Reader) Summary(ctx context.Context, course *int64) ([]Coverage, error) {
	// Counts aggregate with count(CASE...) so both drivers compute the same
	// values (FILTER has no sqlite support). The NULL course guard keeps its
	// ($1 IS NULL OR ...) shape, which both engines evaluate.
	rows, err := s.Pool.Query(
		ctx,
		`SELECT c.id,count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END),count(CASE WHEN d.status='ready' THEN 1 END),count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END),count(CASE WHEN d.status IN('pending','running') THEN 1 END) FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1 WHERE c.hidden=0 AND ($1::bigint IS NULL OR c.id=$1) GROUP BY c.id ORDER BY c.id`,
		course,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Coverage{}
	for rows.Next() {
		var c Coverage
		if err = rows.Scan(
			&c.CourseID,
			&c.SupportedDocuments,
			&c.IndexedDocuments,
			&c.FailedDocuments,
			&c.PendingDocuments,
		); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

type AdminDocument struct {
	queries.KnowledgeDocument
	ChunkCount     int64 `json:"chunk_count"`
	EmbeddingCount int64 `json:"embedding_count"`
}

func (s Reader) Documents(
	ctx context.Context,
	course *int64,
	status, query string,
	limit int,
) ([]AdminDocument, error) {
	var requested []int64
	if course != nil {
		requested = []int64{*course}
	}
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	// The admin list selects explicit document columns plus chunk/embedding
	// counts as scalar subselects; rows scan into AdminDocument fields
	// directly so the query stays portable: to_jsonb(d) has no sqlite form
	// and fails at prepare time. The filter keeps =ANY($N), which the sqlite
	// driver expands to IN lists. Timestamps sort with coalesce on both
	// drivers (empty string sorts first on either backend).
	rows, err := s.Pool.Query(
		ctx,
		`SELECT `+documentColumns+`,(SELECT count(*) FROM knowledge.chunks c WHERE c.document_id=d.id),(SELECT count(DISTINCT c.id) FROM knowledge.chunks c JOIN knowledge.chunk_embeddings e ON e.chunk_id=c.id WHERE c.document_id=d.id) FROM knowledge.documents d WHERE course_id=ANY($1::bigint[]) AND ($2='' OR status=$2) AND ($3='' OR strpos(lower(display_name),lower($3))>0 OR strpos(lower(source_path),lower($3))>0) ORDER BY coalesce(indexed_at,'') DESC,source_path LIMIT $4`,
		ids,
		status,
		identity.Encode(query),
		min(max(1, limit), 500),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []AdminDocument{}
	for rows.Next() {
		var item AdminDocument
		if err = scanDocument(rows, &item); err != nil {
			return nil, err
		}
		for _, text := range []*string{
			&item.CourseName,
			&item.DisplayName,
			&item.SourcePath,
			&item.NormalizedPath,
			item.CourseShortName,
			item.SourceUrl,
			item.Error,
		} {
			if text != nil {
				*text = identity.Decode(*text)
			}
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// documentColumns lists knowledge.documents in KnowledgeDocument scan order
// (see scanDocument); materials.go shares both.
const documentColumns = `d.id,d.course_id,d.course_name,d.course_short_name,d.source_path,d.source_origin,d.normalized_path,d.source_url,d.display_name,d.source_hash,d.source_fingerprint,d.source_etag,d.content_hash_verified,d.mime_type,d.response_mime_type,d.document_kind,d.academic_year,d.source_modified_at,d.is_current,d.status,d.page_count,d.source_size_bytes,d.character_count,d.word_count,d.reading_minutes,d.complexity_score,d.complexity_label,d.language_hint,d.extractor_name,d.extractor_version,d.indexed_at,d.error,d.diagnostic_reason,d.warnings_json`

// scanDocument scans one explicit-column document row plus trailing
// chunk/embedding counts into item. TEXT timestamps bind to *string on both
// drivers; the driver widens COUNT subselects to int64 via the *any targets
// the same way explicit count columns do.
func scanDocument(rows rdbms.Rows, item *AdminDocument) error {
	doc := &item.KnowledgeDocument
	return rows.Scan(
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
		&item.ChunkCount,
		&item.EmbeddingCount,
	)
}
