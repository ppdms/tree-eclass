package rdbms

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/database"
)

func (d postgresDocuments) ReadyDocumentHash(ctx context.Context, course int64,
	document string) (string, *int64, error) {
	var hash string
	var pages *int64
	err := d.db.QueryRow(ctx, `SELECT d.source_hash,d.page_count FROM knowledge.documents d`+
		` JOIN app.courses c ON c.id=d.course_id WHERE d.id=$1 AND d.course_id=$2 AND `+
		documentAdmissionPG+` AND d.status='ready'`+
		` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p`+
		` WHERE p.course_id=c.id AND p.enabled=1))`, document, course).Scan(&hash, &pages)
	return hash, pages, err
}

func (d postgresDocuments) FileGuideHash(ctx context.Context, course int64, document string) (string, error) {
	var hash string
	err := d.db.QueryRow(ctx, `SELECT d.source_hash FROM knowledge.documents d`+
		` JOIN app.courses c ON c.id=d.course_id`+
		` WHERE d.id=$1 AND d.course_id=$2 AND d.is_current=1 AND d.status='ready' AND c.hidden=0`,
		document, course).Scan(&hash)
	return hash, err
}

func (d postgresDocuments) FileMetadataRows(ctx context.Context,
	params database.FileMetadataParams) ([]database.FileMetadataRow, error) {
	rows, err := d.db.Query(ctx, `WITH target AS (`+
		` SELECT d.* FROM knowledge.documents d WHERE d.course_id=$1 AND d.is_current=1), pages AS (`+
		` SELECT p.document_id,count(*) total,count(*) FILTER(WHERE p.status='ready') ready`+
		` FROM knowledge.page_enrichments p JOIN target d ON d.id=p.document_id`+
		` WHERE d.status='ready' AND p.source_hash=d.source_hash AND p.analysis_version=$3`+
		` AND p.requested_model=$2 GROUP BY p.document_id)`+
		` SELECT d.id,d.source_path,d.source_hash,d.document_kind,d.status,d.diagnostic_reason,`+
		`left(d.error,300),d.page_count,d.reading_minutes,d.complexity_label,`+
		`coalesce(e.status,'not_queued'),e.model,e.analysis_version,e.generated_at,`+
		`CASE WHEN e.status='failed' THEN left(e.error,300) END,`+
		`CASE WHEN e.status='ready' AND CASE WHEN pg_input_is_valid(e.payload_json,'jsonb')`+
		` THEN jsonb_typeof(e.payload_json::jsonb->'summary')='string'`+
		` AND length(trim(e.payload_json::jsonb->>'summary'))>0 ELSE false END`+
		` THEN 1 ELSE 0 END,`+
		`coalesce(p.ready,0),coalesce(p.total,0)`+
		` FROM target d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id`+
		` AND d.status='ready' AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=$2`+
		` AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $5 ELSE $4 END`+
		` LEFT JOIN pages p ON p.document_id=d.id ORDER BY d.source_path,d.id`,
		params.Course, params.Model, params.PageVersion, params.DocumentVersion, params.SynthesisVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectFileMetadata(rows)
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

func (d postgresDocuments) Coverage(ctx context.Context, course *int64) ([]database.CoverageRow, error) {
	rows, err := d.db.Query(ctx, `SELECT c.id,`+
		`count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END),`+
		`count(CASE WHEN d.status='ready' THEN 1 END),`+
		`count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END),`+
		`count(CASE WHEN d.status IN('pending','running') THEN 1 END)`+
		` FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1`+
		` WHERE c.hidden=0 AND ($1::bigint IS NULL OR c.id=$1) GROUP BY c.id ORDER BY c.id`, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCoverage(rows)
}

func coursesArgs(courses []int64) []any {
	args := make([]any, 0, len(courses))
	for _, id := range courses {
		args = append(args, id)
	}
	return args
}

func (d postgresDocuments) StatusCoverage(ctx context.Context, courses []int64) ([]database.CoverageRow, error) {
	args := coursesArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT c.id,`+
		`count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END),`+
		`count(CASE WHEN d.status='ready' THEN 1 END),`+
		`count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END),`+
		`count(CASE WHEN d.status IN('pending','running') THEN 1 END)`+
		` FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1`+
		` WHERE c.id IN`+pgPlaceholders(1, len(courses))+` GROUP BY c.id ORDER BY c.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCoverage(rows)
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

func (d postgresDocuments) StatusCounts(ctx context.Context, courses []int64) ([]database.StatusCount, error) {
	args := coursesArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT status,count(*) FROM knowledge.documents`+
		` WHERE course_id IN`+pgPlaceholders(1, len(courses))+` AND is_current=1`+
		` GROUP BY status ORDER BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d postgresDocuments) IndexJobCounts(ctx context.Context, courses []int64) ([]database.StatusCount, error) {
	args := coursesArgs(courses)
	rows, err := d.db.Query(ctx, `SELECT q.status,count(*)`+
		` FROM app.control_commands q JOIN knowledge.documents d ON d.id=q.payload->>'document_id'`+
		` WHERE q.queue='index' AND d.course_id IN`+pgPlaceholders(1, len(courses))+` AND d.is_current=1`+
		` GROUP BY q.status ORDER BY q.status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d postgresDocuments) FailedDocuments(ctx context.Context, courses []int64,
	limit int) ([]database.DiagnosticDocument, error) {
	args := append(coursesArgs(courses), limit)
	rows, err := d.db.Query(ctx, `SELECT id,course_id,display_name,source_path,status,diagnostic_reason,`+
		`left(error,1000) FROM knowledge.documents WHERE course_id IN`+pgPlaceholders(1, len(courses))+
		` AND is_current=1 AND status='failed' ORDER BY course_id,source_path LIMIT $`+
		strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDiagnostics(rows)
}

func (d postgresDocuments) UnsupportedDocuments(ctx context.Context, courses []int64,
	limit int) ([]database.DiagnosticDocument, error) {
	args := append(coursesArgs(courses), limit)
	rows, err := d.db.Query(ctx, `SELECT id,course_id,display_name,source_path,status,diagnostic_reason,`+
		`left(error,1000) FROM knowledge.documents WHERE course_id IN`+pgPlaceholders(1, len(courses))+
		` AND is_current=1 AND status IN('unsupported','skipped_limit')`+
		` ORDER BY course_id,source_path LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDiagnostics(rows)
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
