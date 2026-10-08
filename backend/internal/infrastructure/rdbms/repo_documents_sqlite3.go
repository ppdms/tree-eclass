package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func (d sqliteDocuments) ReadyDocumentHash(ctx context.Context, course int64,
	document string) (string, *int64, error) {
	var hash string
	var pages *int64
	err := d.db.QueryRow(ctx, `SELECT d.source_hash,d.page_count FROM documents d`+
		` JOIN courses c ON c.id=d.course_id WHERE d.id=? AND d.course_id=? AND `+
		documentAdmissionSQLite+` AND d.status='ready'`+
		` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p`+
		` WHERE p.course_id=c.id AND p.enabled=1))`, document, course).Scan(&hash, &pages)
	return hash, pages, err
}

func (d sqliteDocuments) FileGuideHash(ctx context.Context, course int64, document string) (string, error) {
	var hash string
	err := d.db.QueryRow(ctx, `SELECT d.source_hash FROM documents d`+
		` JOIN courses c ON c.id=d.course_id`+
		` WHERE d.id=? AND d.course_id=? AND d.is_current=1 AND d.status='ready' AND c.hidden=0`,
		document, course).Scan(&hash)
	return hash, err
}

func (d sqliteDocuments) FileMetadataRows(ctx context.Context,
	params database.FileMetadataParams) ([]database.FileMetadataRow, error) {
	// FILTER and pg_input_is_valid/jsonb_typeof have no SQLite form. Ready
	// counts use count(CASE...); the usable-summary predicate runs natively
	// over JSON1: valid JSON object with a non-blank string summary.
	rows, err := d.db.Query(ctx, `WITH target AS (`+
		` SELECT d.* FROM documents d WHERE d.course_id=? AND d.is_current=1), pages AS (`+
		` SELECT p.document_id,count(*) total,count(CASE WHEN p.status='ready' THEN 1 END) ready`+
		` FROM page_enrichments p JOIN target d ON d.id=p.document_id`+
		` WHERE d.status='ready' AND p.source_hash=d.source_hash AND p.analysis_version=?`+
		` AND p.requested_model=? GROUP BY p.document_id)`+
		` SELECT d.id,d.source_path,d.source_hash,d.document_kind,d.status,d.diagnostic_reason,`+
		`substr(d.error,1,300),d.page_count,d.reading_minutes,d.complexity_label,`+
		`coalesce(e.status,'not_queued'),e.model,e.analysis_version,e.generated_at,`+
		`CASE WHEN e.status='failed' THEN substr(e.error,1,300) END,`+
		`CASE WHEN e.status='ready' AND json_valid(e.payload_json) THEN coalesce(`+
		` json_type(e.payload_json,'$.summary')='text'`+
		` AND length(trim(json_extract(e.payload_json,'$.summary')))>0,0) ELSE 0 END,`+
		`coalesce(p.ready,0),coalesce(p.total,0)`+
		` FROM target d LEFT JOIN document_enrichments e ON e.document_id=d.id`+
		` AND d.status='ready' AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=?`+
		` AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END`+
		` LEFT JOIN pages p ON p.document_id=d.id ORDER BY d.source_path,d.id`,
		params.Course, params.PageVersion, params.Model, params.Model,
		params.SynthesisVersion, params.DocumentVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectFileMetadata(rows)
}

func (d sqliteDocuments) Coverage(ctx context.Context, course *int64) ([]database.CoverageRow, error) {
	rows, err := d.db.Query(ctx, `SELECT c.id,`+
		`count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END),`+
		`count(CASE WHEN d.status='ready' THEN 1 END),`+
		`count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END),`+
		`count(CASE WHEN d.status IN('pending','running') THEN 1 END)`+
		` FROM courses c LEFT JOIN documents d ON d.course_id=c.id AND d.is_current=1`+
		` WHERE c.hidden=0 AND (? IS NULL OR c.id=?) GROUP BY c.id ORDER BY c.id`, course, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCoverage(rows)
}

func (d sqliteDocuments) StatusCoverage(ctx context.Context, courses []int64) ([]database.CoverageRow, error) {
	args := sqliteCourseArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT c.id,`+
		`count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END),`+
		`count(CASE WHEN d.status='ready' THEN 1 END),`+
		`count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END),`+
		`count(CASE WHEN d.status IN('pending','running') THEN 1 END)`+
		` FROM courses c LEFT JOIN documents d ON d.course_id=c.id AND d.is_current=1`+
		` WHERE c.id IN`+sqlitePlaceholders(len(courses))+` GROUP BY c.id ORDER BY c.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCoverage(rows)
}

func (d sqliteDocuments) StatusCounts(ctx context.Context, courses []int64) ([]database.StatusCount, error) {
	args := sqliteCourseArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT status,count(*) FROM documents`+
		` WHERE course_id IN`+sqlitePlaceholders(len(courses))+` AND is_current=1`+
		` GROUP BY status ORDER BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d sqliteDocuments) IndexJobCounts(ctx context.Context, courses []int64) ([]database.StatusCount, error) {
	args := sqliteCourseArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT q.status,count(*)`+
		` FROM control_commands q JOIN documents d ON d.id=json_extract(q.payload,'$.document_id')`+
		` WHERE q.queue='index' AND d.course_id IN`+sqlitePlaceholders(len(courses))+` AND d.is_current=1`+
		` GROUP BY q.status ORDER BY q.status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d sqliteDocuments) FailedDocuments(ctx context.Context, courses []int64,
	limit int) ([]database.DiagnosticDocument, error) {
	args := append(sqliteCourseArgs(courses), limit)
	rows, err := d.db.Query(ctx, `SELECT id,course_id,display_name,source_path,status,diagnostic_reason,`+
		`substr(error,1,1000) FROM documents WHERE course_id IN`+sqlitePlaceholders(len(courses))+
		` AND is_current=1 AND status='failed' ORDER BY course_id,source_path LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDiagnostics(rows)
}

func (d sqliteDocuments) UnsupportedDocuments(ctx context.Context, courses []int64,
	limit int) ([]database.DiagnosticDocument, error) {
	args := append(sqliteCourseArgs(courses), limit)
	rows, err := d.db.Query(ctx, `SELECT id,course_id,display_name,source_path,status,diagnostic_reason,`+
		`substr(error,1,1000) FROM documents WHERE course_id IN`+sqlitePlaceholders(len(courses))+
		` AND is_current=1 AND status IN('unsupported','skipped_limit')`+
		` ORDER BY course_id,source_path LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDiagnostics(rows)
}
