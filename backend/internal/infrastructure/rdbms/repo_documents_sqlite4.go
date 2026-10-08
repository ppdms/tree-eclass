package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func guideStatusCTESQLite(courses int) string {
	return `WITH guides AS (` +
		` SELECT d.id document_id,d.course_id,d.display_name,d.source_path,e.model,substr(e.error,1,1000) error,` +
		` CASE WHEN e.document_id IS NULL THEN 'not_queued'` +
		` WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>?` +
		` OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END` +
		` THEN 'stale' ELSE e.status END status,` +
		` CASE WHEN e.document_id IS NULL THEN 'not_queued'` +
		` WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>?` +
		` OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END` +
		` THEN 'stale_generation' WHEN e.status='failed' THEN 'generation_failed'` +
		` WHEN e.status='running' THEN 'processing' WHEN e.status='pending' THEN 'queued' ELSE 'ready' END reason` +
		` FROM documents d LEFT JOIN document_enrichments e ON e.document_id=d.id` +
		` WHERE d.course_id IN` + sqlitePlaceholders(courses) +
		` AND d.is_current=1 AND d.status='ready' AND d.document_kind<>'archive') `
}

func guideArgsSQLite(params database.GuideFreshnessParams, extra ...any) []any {
	args := make([]any, 0, len(params.Courses)+6+len(extra))
	args = append(args, params.Model, params.SynthesisVersion, params.DocumentVersion,
		params.Model, params.SynthesisVersion, params.DocumentVersion)
	for _, id := range params.Courses {
		args = append(args, id)
	}
	return append(args, extra...)
}

func (d sqliteDocuments) GuideSummary(ctx context.Context,
	params database.GuideFreshnessParams) ([]database.StatusCount, error) {
	args := guideArgsSQLite(params)
	rows, err := d.db.Query(ctx, guideStatusCTESQLite(len(params.Courses))+
		`SELECT status,count(*) FROM guides GROUP BY status ORDER BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d sqliteDocuments) GuideDiagnostics(ctx context.Context, params database.GuideFreshnessParams,
	limit int) ([]database.GuideDiagnostic, error) {
	args := guideArgsSQLite(params, limit)
	rows, err := d.db.Query(ctx, guideStatusCTESQLite(len(params.Courses))+
		`SELECT document_id,course_id,display_name,source_path,model,error,status,reason FROM guides`+
		` WHERE status<>'ready' ORDER BY course_id,source_path LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.GuideDiagnostic{}
	for rows.Next() {
		var item database.GuideDiagnostic
		if err := rows.Scan(&item.DocumentID, &item.CourseID, &item.Display, &item.Path,
			&item.Model, &item.Error, &item.Status, &item.Reason); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d sqliteDocuments) EmbeddingCounts(ctx context.Context, courses []int64,
	model string) (int64, int64, error) {
	args := append([]any{model}, sqliteCourseArgs(courses)...)
	var chunks, embedded int64
	err := d.db.QueryRow(ctx, `SELECT count(*),count(CASE WHEN EXISTS(`+
		`SELECT 1 FROM chunk_embeddings e WHERE e.chunk_id=c.id AND e.model=?) THEN 1 END)`+
		` FROM chunks c JOIN documents d ON d.id=c.document_id`+
		` WHERE d.course_id IN`+sqlitePlaceholders(len(courses))+` AND d.is_current=1 AND d.status='ready'`,
		args...).Scan(&chunks, &embedded)
	return chunks, embedded, err
}

func (d sqliteDocuments) ReadinessCounts(ctx context.Context,
	params database.ReadinessParams) ([]database.ReadinessCount, error) {
	// readinessQuerySQLite positions: model, synthesis, document, course,
	// page version, model. SQLite binds positionally.
	rows, err := d.db.Query(ctx, readinessQuerySQLite, params.Model, params.SynthesisVersion,
		params.DocumentVersion, params.Course, params.PageVersion, params.Model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ReadinessCount{}
	for rows.Next() {
		var item database.ReadinessCount
		if err := rows.Scan(&item.Category, &item.Status, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d sqliteDocuments) BlueprintStatus(ctx context.Context, course int64, model, version string) (string, error) {
	var status string
	err := d.db.QueryRow(ctx, `SELECT coalesce((SELECT b.status FROM course_blueprints b`+
		` WHERE b.course_id=? AND b.requested_model=? AND b.analysis_version=?`+
		` ORDER BY revision DESC LIMIT 1),'missing')`, course, model, version).Scan(&status)
	return status, err
}

func (d sqliteDocuments) RecentChanges(ctx context.Context, courses []int64, since string,
	limit int) ([]database.ChangeItem, error) {
	args := append(sqliteCourseArgs(courses), since, limit)
	rows, err := d.db.Query(ctx, `SELECT r.course_id,c.name,r.timestamp,r.change_no,i.change_type,`+
		`i.file_path,i.display_name,i.redirect_url,i.diff_webdav_path`+
		` FROM change_record_items i JOIN change_records r ON r.id=i.change_record_id`+
		` JOIN courses c ON c.id=r.course_id AND c.hidden=0`+
		` WHERE r.course_id IN`+sqlitePlaceholders(len(courses))+` AND r.timestamp>=?`+
		` ORDER BY r.timestamp DESC,i.id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ChangeItem{}
	for rows.Next() {
		var item database.ChangeItem
		if err := rows.Scan(&item.CourseID, &item.CourseName, &item.Timestamp, &item.ChangeNo,
			&item.ChangeType, &item.FilePath, &item.DisplayName, &item.RedirectURL, &item.DiffWebdav); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
