package rdbms

import (
	"context"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
)

const guideStatusCTEPG = `WITH guides AS (` +
	` SELECT d.id document_id,d.course_id,d.display_name,d.source_path,e.model,left(e.error,1000) error,` +
	` CASE WHEN e.document_id IS NULL THEN 'not_queued'` +
	` WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>$N2` +
	` OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $N4 ELSE $N3 END` +
	` THEN 'stale' ELSE e.status END status,` +
	` CASE WHEN e.document_id IS NULL THEN 'not_queued'` +
	` WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>$N2` +
	` OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $N4 ELSE $N3 END` +
	` THEN 'stale_generation' WHEN e.status='failed' THEN 'generation_failed'` +
	` WHEN e.status='running' THEN 'processing' WHEN e.status='pending' THEN 'queued' ELSE 'ready' END reason` +
	` FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id` +
	` WHERE d.course_id IN` + `COURSES` + ` AND d.is_current=1 AND d.status='ready' AND d.document_kind<>'archive') `

func (d postgresDocuments) GuideSummary(ctx context.Context,
	params database.GuideFreshnessParams) ([]database.StatusCount, error) {
	args := make([]any, 0, len(params.Courses)+3)
	for _, id := range params.Courses {
		args = append(args, id)
	}
	n := len(args) + 1
	args = append(args, params.Model, params.DocumentVersion, params.SynthesisVersion)
	query := guideStatusCTEPG
	query = strings.ReplaceAll(query, "COURSES", pgPlaceholders(1, len(params.Courses)))
	query = strings.ReplaceAll(query, "$N2", "$"+strconv.Itoa(n))
	query = strings.ReplaceAll(query, "$N3", "$"+strconv.Itoa(n+1))
	query = strings.ReplaceAll(query, "$N4", "$"+strconv.Itoa(n+2))
	rows, err := d.db.Query(ctx, query+`SELECT status,count(*) FROM guides GROUP BY status ORDER BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStatusCounts(rows)
}

func (d postgresDocuments) GuideDiagnostics(ctx context.Context, params database.GuideFreshnessParams,
	limit int) ([]database.GuideDiagnostic, error) {
	args := make([]any, 0, len(params.Courses)+4)
	for _, id := range params.Courses {
		args = append(args, id)
	}
	n := len(args) + 1
	args = append(args, params.Model, params.DocumentVersion, params.SynthesisVersion, limit)
	query := guideStatusCTEPG
	query = strings.ReplaceAll(query, "COURSES", pgPlaceholders(1, len(params.Courses)))
	query = strings.ReplaceAll(query, "$N2", "$"+strconv.Itoa(n))
	query = strings.ReplaceAll(query, "$N3", "$"+strconv.Itoa(n+1))
	query = strings.ReplaceAll(query, "$N4", "$"+strconv.Itoa(n+2))
	rows, err := d.db.Query(ctx, query+
		`SELECT document_id,course_id,display_name,source_path,model,error,status,reason FROM guides`+
		` WHERE status<>'ready' ORDER BY course_id,source_path LIMIT $`+strconv.Itoa(len(args)), args...)
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

func (d postgresDocuments) EmbeddingCounts(ctx context.Context, courses []int64,
	model string) (int64, int64, error) {
	args := append(coursesArgs(courses), model)
	var chunks, embedded int64
	err := d.db.QueryRow(ctx, `SELECT count(*),count(CASE WHEN EXISTS(`+
		`SELECT 1 FROM knowledge.chunk_embeddings e WHERE e.chunk_id=c.id AND e.model=$`+
		strconv.Itoa(len(args))+`) THEN 1 END) FROM knowledge.chunks c`+
		` JOIN knowledge.documents d ON d.id=c.document_id`+
		` WHERE d.course_id IN`+pgPlaceholders(1, len(courses))+` AND d.is_current=1 AND d.status='ready'`,
		args...).Scan(&chunks, &embedded)
	return chunks, embedded, err
}

func (d postgresDocuments) ReadinessCounts(ctx context.Context,
	params database.ReadinessParams) ([]database.ReadinessCount, error) {
	rows, err := d.db.Query(ctx, readinessQueryPG, params.Course, params.Model,
		params.DocumentVersion, params.SynthesisVersion, params.PageVersion)
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

func (d postgresDocuments) BlueprintStatus(ctx context.Context, course int64, model, version string) (string, error) {
	var status string
	err := d.db.QueryRow(ctx, `SELECT coalesce((SELECT b.status FROM knowledge.course_blueprints b`+
		` WHERE b.course_id=$1 AND b.requested_model=$2 AND b.analysis_version=$3`+
		` ORDER BY revision DESC LIMIT 1),'missing')`, course, model, version).Scan(&status)
	return status, err
}

func (d postgresDocuments) RecentChanges(ctx context.Context, courses []int64, since string,
	limit int) ([]database.ChangeItem, error) {
	args := append(coursesArgs(courses), since, limit)
	rows, err := d.db.Query(ctx, `SELECT r.course_id,c.name,r.timestamp,r.change_no,i.change_type,`+
		`i.file_path,i.display_name,i.redirect_url,i.diff_webdav_path`+
		` FROM app.change_record_items i JOIN app.change_records r ON r.id=i.change_record_id`+
		` JOIN app.courses c ON c.id=r.course_id AND c.hidden=0`+
		` WHERE r.course_id IN`+pgPlaceholders(1, len(courses))+` AND r.timestamp>=$`+
		strconv.Itoa(len(args)-1)+` ORDER BY r.timestamp DESC,i.id DESC LIMIT $`+strconv.Itoa(len(args)), args...)
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
