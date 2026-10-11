package rdbms

import (
	"strings"

	"tree-eclass/internal/domain/database"
)

// documentAdmissionSQLite enforces the immutable source boundary for alias d:
// current eclass/external membership, live catalog revision agreeing with the
// content hash, and every archive ancestor agreeing with current identity.
const documentAdmissionSQLite = `d.is_current=1 AND d.source_origin IN('eclass','external')` +
	` AND EXISTS(SELECT 1 FROM document_revisions r JOIN objects o ON o.id=r.object_id` +
	` WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path` +
	` AND r.deleted_at IS NULL AND o.sha256=d.source_hash)` +
	` AND NOT EXISTS(WITH RECURSIVE source_ancestors AS(` +
	` SELECT d.id current,','||d.id||',' trail,0 depth` +
	` UNION ALL SELECT m.parent_document_id,a.trail||m.parent_document_id||',',a.depth+1` +
	` FROM source_ancestors a JOIN archive_members m ON m.child_document_id=a.current` +
	` WHERE instr(a.trail,','||m.parent_document_id||',')=0 AND a.depth<8)` +
	` SELECT 1 FROM source_ancestors a JOIN archive_members m ON m.child_document_id=a.current` +
	` LEFT JOIN documents child ON child.id=m.child_document_id` +
	` LEFT JOIN documents p ON p.id=m.parent_document_id` +
	` WHERE p.id IS NULL OR p.course_id<>d.course_id OR p.is_current<>1 OR p.status<>'ready'` +
	` OR p.content_hash_verified<>1 OR p.source_origin NOT IN('eclass','external')` +
	` OR m.parent_source_hash<>p.source_hash OR m.member_hash<>child.source_hash` +
	` OR instr(a.trail,','||m.parent_document_id||',')>0 OR a.depth>=8` +
	` OR NOT EXISTS(SELECT 1 FROM document_revisions r JOIN objects o ON o.id=r.object_id` +
	` WHERE r.document_id=p.id AND r.course_id=p.course_id AND r.logical_path=p.normalized_path` +
	` AND r.deleted_at IS NULL AND o.sha256=p.source_hash))`

// documentColumnsSQLite lists the documents columns in KnowledgeDocument
// scan order.
const documentColumnsSQLite = `d.id,d.course_id,d.course_name,d.course_short_name,d.source_path,d.source_origin,` +
	`d.normalized_path,d.source_url,d.display_name,d.source_hash,d.source_fingerprint,d.source_etag,` +
	`d.content_hash_verified,d.mime_type,d.response_mime_type,d.document_kind,d.academic_year,` +
	`d.source_modified_at,d.is_current,d.status,d.page_count,d.source_size_bytes,d.character_count,` +
	`d.word_count,d.reading_minutes,d.complexity_score,d.complexity_label,d.language_hint,` +
	`d.extractor_name,d.extractor_version,d.indexed_at,d.error,d.diagnostic_reason,d.warnings_json`

// sqlitePlaceholders renders (?,...,?) for IN lists; empty renders (NULL).
func sqlitePlaceholders(n int) string {
	if n == 0 {
		return "(NULL)"
	}
	return "(" + strings.TrimRight(strings.Repeat("?,", n), ",") + ")"
}

// readinessQuerySQLite aggregates readiness counters with JSON1 extraction.
// docs carries the enrichment join plus the source-admission flag; evidence
// narrows to servable documents.
const readinessQuerySQLite = `WITH docs AS (` +
	` SELECT d.*,e.status enrichment_status, (` + documentAdmissionSQLite + `) source_admitted` +
	` FROM documents d LEFT JOIN document_enrichments e` +
	` ON e.document_id=d.id AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=?` +
	` AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END` +
	` WHERE d.course_id=? AND d.is_current=1` +
	`), evidence AS (SELECT * FROM docs WHERE status='ready' AND document_kind<>'archive'` +
	` AND content_hash_verified=1 AND source_admitted), counts AS (` +
	` SELECT 'documents' category,status,count(*) n FROM docs GROUP BY status` +
	` UNION ALL SELECT 'extraction_jobs',c.status,count(*) FROM control_commands c` +
	` JOIN docs d ON d.id=json_extract(c.payload,'$.document_id') WHERE c.queue='index' GROUP BY c.status` +
	` UNION ALL SELECT 'document_enrichments',enrichment_status,count(*) FROM evidence` +
	` WHERE enrichment_status IS NOT NULL GROUP BY enrichment_status` +
	` UNION ALL SELECT 'page_enrichments',p.status,count(*) FROM page_enrichments p` +
	` JOIN evidence d ON d.id=p.document_id AND d.source_hash=p.source_hash` +
	` WHERE p.analysis_version=? AND p.requested_model=? GROUP BY p.status` +
	` UNION ALL SELECT 'ready','',count(*) FROM evidence` +
	` UNION ALL SELECT 'unverified','',count(*) FROM docs WHERE status='ready'` +
	` AND document_kind<>'archive' AND (content_hash_verified<>1 OR NOT source_admitted)` +
	` UNION ALL SELECT 'missing','',count(*) FROM evidence WHERE enrichment_status IS NULL` +
	`) SELECT category,status,n FROM counts`

// scanDocumentPointers returns scan targets for a full document row.
func scanDocumentPointers(doc *database.KnowledgeDocument) []any {
	return []any{
		&doc.ID, &doc.CourseID, &doc.CourseName, &doc.CourseShortName, &doc.SourcePath,
		&doc.SourceOrigin, &doc.NormalizedPath, &doc.SourceUrl, &doc.DisplayName, &doc.SourceHash,
		&doc.SourceFingerprint, &doc.SourceEtag, &doc.ContentHashVerified, &doc.MimeType,
		&doc.ResponseMimeType, &doc.DocumentKind, &doc.AcademicYear, &doc.SourceModifiedAt,
		&doc.IsCurrent, &doc.Status, &doc.PageCount, &doc.SourceSizeBytes, &doc.CharacterCount,
		&doc.WordCount, &doc.ReadingMinutes, &doc.ComplexityScore, &doc.ComplexityLabel,
		&doc.LanguageHint, &doc.ExtractorName, &doc.ExtractorVersion, &doc.IndexedAt,
		&doc.Error, &doc.DiagnosticReason, &doc.WarningsJson,
	}
}

func scanSearchCandidate(rows nativeRows, withVector bool) (database.EmbeddedCandidate, error) {
	var out database.EmbeddedCandidate
	doc := &out.Document
	pointers := []any{
		&doc.ID, &doc.CourseID, &doc.CourseName, &doc.CourseShortName, &doc.SourcePath,
		&doc.SourceOrigin, &doc.NormalizedPath, &doc.SourceUrl, &doc.DisplayName, &doc.SourceHash,
		&doc.MimeType, &doc.ResponseMimeType, &doc.DocumentKind, &doc.AcademicYear,
		&doc.SourceModifiedAt, &doc.IndexedAt,
		&out.ChunkID, &out.Ordinal, &out.LocatorType, &out.LocatorStart, &out.LocatorEnd,
		&out.Heading, &out.MetadataJSON, &out.Excerpt, &out.Text,
	}
	if withVector {
		pointers = append(pointers, &out.Vector)
	}
	return out, rows.Scan(pointers...)
}

func collectFileMetadata(rows nativeRows) ([]database.FileMetadataRow, error) {
	out := []database.FileMetadataRow{}
	for rows.Next() {
		var item database.FileMetadataRow
		var guide int64
		if err := rows.Scan(&item.ID, &item.Path, &item.Hash, &item.Kind, &item.Status,
			&item.Reason, &item.Error, &item.Pages, &item.Minutes, &item.Complexity,
			&item.Analysis, &item.Model, &item.Version, &item.GeneratedAt, &item.AnalysisError,
			&guide, &item.PagesReady, &item.PagesTotal); err != nil {
			return nil, err
		}
		item.Guide = guide != 0
		out = append(out, item)
	}
	return out, rows.Err()
}

func scanCoverage(rows nativeRows) ([]database.CoverageRow, error) {
	out := []database.CoverageRow{}
	for rows.Next() {
		var item database.CoverageRow
		if err := rows.Scan(&item.CourseID, &item.Supported, &item.Indexed, &item.Failed, &item.Pending); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func scanStatusCounts(rows nativeRows) ([]database.StatusCount, error) {
	out := []database.StatusCount{}
	for rows.Next() {
		var item database.StatusCount
		if err := rows.Scan(&item.Status, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func scanDiagnostics(rows nativeRows) ([]database.DiagnosticDocument, error) {
	out := []database.DiagnosticDocument{}
	for rows.Next() {
		var item database.DiagnosticDocument
		if err := rows.Scan(&item.DocumentID, &item.CourseID, &item.Display, &item.Path,
			&item.Status, &item.Reason, &item.Error); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
