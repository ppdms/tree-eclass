package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
)

func (a sqliteAnalysis) QueuePages(ctx context.Context,
	params database.QueuePagesParams) error {
	if params.Pages < 1 {
		return nil
	}
	rows := make([]string, 0, params.Pages)
	args := make([]any, 0, params.Pages*7)
	for page := range params.Pages {
		rows = append(rows, `SELECT ?,?, ?,?,'pending',?,?,?`)
		args = append(args, params.DocumentID, page+1, params.SourceHash,
			params.Version, params.Model, params.Model, params.AvailableAt)
	}
	_, err := a.db.Exec(ctx, `INSERT INTO page_enrichments`+
		`(document_id,page_number,source_hash,analysis_version,status,model,requested_model,available_at)`+
		` `+strings.Join(rows, " UNION ALL ")+
		` ON CONFLICT(document_id,page_number) DO UPDATE`+
		` SET source_hash=excluded.source_hash,analysis_version=excluded.analysis_version,`+
		`status='pending',model=excluded.model,requested_model=excluded.requested_model,`+
		`available_at=excluded.available_at,attempts=0,claimed_at=NULL,payload_json=NULL,error=NULL`+
		` WHERE page_enrichments.source_hash<>excluded.source_hash`+
		` OR page_enrichments.analysis_version<>excluded.analysis_version`+
		` OR page_enrichments.requested_model<>excluded.requested_model`,
		args...)
	return err
}

func (a sqliteAnalysis) SampleExcerpts(ctx context.Context,
	document string) ([]database.ExcerptRow, error) {
	// Twelve samples mirror the postgres round((total-1)/11*k)+1 row-number
	// selection over k=0..11.
	rows, err := a.db.Query(ctx, `WITH ranked AS(SELECT *,row_number() OVER(ORDER BY ordinal) n,`+
		`count(*) OVER() total FROM chunks WHERE document_id=?)`+
		` SELECT locator_type,coalesce(locator_start,''),substr(text,1,2500) FROM ranked`+
		` WHERE n IN(SELECT CAST(round(column1*(total-1)/11.0)+1 AS INTEGER)`+
		` FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11))) ORDER BY ordinal`,
		document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanExcerpts(rows)
}

func (a sqliteAnalysis) PageExcerpts(ctx context.Context, document,
	page string) ([]database.ExcerptRow, error) {
	rows, err := a.db.Query(ctx, `SELECT locator_type,coalesce(locator_start,''),substr(text,1,12000)`+
		` FROM chunks WHERE document_id=? AND locator_type='page'`+
		` AND locator_start=? ORDER BY ordinal`, document, page)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanExcerpts(rows)
}

func (a sqliteAnalysis) PageImageObject(ctx context.Context, document string,
	course int64, path, hash string) (database.ObjectReference, error) {
	var out database.ObjectReference
	err := a.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type`+
		` FROM document_revisions r JOIN objects o ON o.id=r.object_id`+
		` WHERE r.document_id=? AND r.course_id=? AND r.logical_path=?`+
		` AND r.deleted_at IS NULL AND o.sha256=? ORDER BY r.created_at DESC LIMIT 1`,
		document, course, path, hash).Scan(
		&out.Bucket, &out.Key, &out.VersionID, &out.SHA256, &out.Bytes, &out.MediaType)
	return out, err
}

func (a sqliteAnalysis) PageEvidence(ctx context.Context, document, hash,
	version, model string, pages int64) ([]database.EvidenceRow, error) {
	rows, err := a.db.Query(ctx, `SELECT page_number,`+
		`CASE WHEN length(CAST(payload_json AS BLOB))<=262144 THEN payload_json END`+
		` FROM page_enrichments WHERE document_id=? AND source_hash=?`+
		` AND analysis_version=? AND requested_model=? AND status='ready'`+
		` AND page_number BETWEEN 1 AND ? ORDER BY page_number`,
		document, hash, version, model, pages)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.EvidenceRow{}
	for rows.Next() {
		var item database.EvidenceRow
		if err := rows.Scan(&item.PageNumber, &item.Payload); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (a sqliteAnalysis) PublishReady(ctx context.Context,
	params database.AnalysisPublishParams) error {
	if params.Page > 0 {
		_, err := a.db.Exec(ctx, `UPDATE page_enrichments`+
			` SET status='ready',model=?,payload_json=?,generated_at=?,error=NULL,claimed_at=NULL`+
			` WHERE document_id=? AND page_number=? AND claimed_at=? AND status='running'`+
			` AND source_hash=? AND requested_model=? AND analysis_version=?`,
			params.Model, params.Payload, params.GeneratedAt,
			params.DocumentID, params.Page, params.ClaimedAt,
			params.Hash, params.Requested, params.Version)
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE document_enrichments`+
		` SET status='ready',model=?,requested_model=?,payload_json=?,generated_at=?,`+
		`error=NULL,claimed_at=NULL WHERE document_id=? AND claimed_at=? AND status='running'`+
		` AND source_hash=? AND context_hash=? AND analysis_version=?`,
		params.Model, params.Requested, params.Payload, params.GeneratedAt,
		params.DocumentID, params.ClaimedAt, params.Hash, params.ContextHash, params.Version)
	return err
}

func (a sqliteAnalysis) FinishClaim(ctx context.Context, params database.AnalysisFinishParams) error {
	if params.Page > 0 {
		_, err := a.db.Exec(ctx, `UPDATE page_enrichments`+
			` SET status=?,error=?,available_at=?,claimed_at=NULL,`+
			`attempts=CASE WHEN ? THEN max(0,attempts-1) ELSE attempts END`+
			` WHERE document_id=? AND page_number=? AND claimed_at=? AND status='running'`,
			params.Status, params.Error, params.AvailableAt, params.Reset,
			params.DocumentID, params.Page, params.ClaimedAt)
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE document_enrichments`+
		` SET status=?,error=?,available_at=?,claimed_at=NULL,`+
		`attempts=CASE WHEN ? THEN max(0,attempts-1) ELSE attempts END`+
		` WHERE document_id=? AND claimed_at=? AND status='running'`,
		params.Status, params.Error, params.AvailableAt, params.Reset,
		params.DocumentID, params.ClaimedAt)
	return err
}

func (a sqliteAnalysis) RecoverLane(ctx context.Context, availableAt string) error {
	if _, err := a.db.Exec(ctx, `UPDATE document_enrichments`+
		` SET status='pending',claimed_at=NULL,attempts=max(0,attempts-1),`+
		`available_at=? WHERE status='running'`, availableAt); err != nil {
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE page_enrichments`+
		` SET status='pending',claimed_at=NULL,attempts=max(0,attempts-1),`+
		`available_at=? WHERE status='running'`, availableAt)
	return err
}
