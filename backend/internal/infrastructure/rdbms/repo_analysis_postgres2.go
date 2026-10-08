package rdbms

import (
	"context"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
)

func (a postgresAnalysis) QueuePages(ctx context.Context,
	params database.QueuePagesParams) error {
	if params.Pages < 1 {
		return nil
	}
	rows := make([]string, 0, params.Pages)
	args := make([]any, 0, params.Pages*6)
	for page := int64(1); page <= params.Pages; page++ {
		base := len(args) + 1
		rows = append(rows, `(SELECT $`+strconv.Itoa(base)+`,$`+strconv.Itoa(base+1)+
			`,$`+strconv.Itoa(base+2)+`,$`+strconv.Itoa(base+3)+`,'pending',$`+
			strconv.Itoa(base+4)+`,$`+strconv.Itoa(base+4)+`,$`+strconv.Itoa(base+5)+`)`)
		args = append(args, params.DocumentID, page, params.SourceHash,
			params.Version, params.Model, params.AvailableAt)
	}
	_, err := a.db.Exec(ctx, `INSERT INTO knowledge.page_enrichments`+
		`(document_id,page_number,source_hash,analysis_version,status,model,requested_model,available_at)`+
		` SELECT * FROM (`+strings.Join(rows, " UNION ALL ")+`) AS pages`+
		`(document_id,page_number,source_hash,analysis_version,status,model,requested_model,available_at)`+
		` ON CONFLICT(document_id,page_number) DO UPDATE`+
		` SET source_hash=excluded.source_hash,analysis_version=excluded.analysis_version,`+
		`status='pending',model=excluded.model,requested_model=excluded.requested_model,`+
		`available_at=excluded.available_at,attempts=0,claimed_at=NULL,payload_json=NULL,error=NULL`+
		` WHERE knowledge.page_enrichments.source_hash<>excluded.source_hash`+
		` OR knowledge.page_enrichments.analysis_version<>excluded.analysis_version`+
		` OR knowledge.page_enrichments.requested_model<>excluded.requested_model`,
		args...)
	return err
}

func (a postgresAnalysis) SampleExcerpts(ctx context.Context,
	document string) ([]database.ExcerptRow, error) {
	rows, err := a.db.Query(ctx, `WITH ranked AS(SELECT *,row_number() OVER(ORDER BY ordinal) n,`+
		`count(*) OVER() total FROM knowledge.chunks WHERE document_id=$1)`+
		` SELECT locator_type,coalesce(locator_start,''),left(text,2500) FROM ranked`+
		` WHERE n IN(SELECT round(column1*(total-1)::numeric/11)+1`+
		` FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11))) ORDER BY ordinal`,
		document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanExcerpts(rows)
}

func (a postgresAnalysis) PageExcerpts(ctx context.Context, document,
	page string) ([]database.ExcerptRow, error) {
	rows, err := a.db.Query(ctx, `SELECT locator_type,coalesce(locator_start,''),left(text,12000)`+
		` FROM knowledge.chunks WHERE document_id=$1 AND locator_type='page'`+
		` AND locator_start=$2 ORDER BY ordinal`, document, page)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanExcerpts(rows)
}

func (a postgresAnalysis) PageImageObject(ctx context.Context, document string,
	course int64, path, hash string) (database.ObjectReference, error) {
	var out database.ObjectReference
	err := a.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type`+
		` FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id`+
		` WHERE r.document_id=$1 AND r.course_id=$2 AND r.logical_path=$3`+
		` AND r.deleted_at IS NULL AND o.sha256=$4 ORDER BY r.created_at DESC LIMIT 1`,
		document, course, path, hash).Scan(
		&out.Bucket, &out.Key, &out.VersionID, &out.SHA256, &out.Bytes, &out.MediaType)
	return out, err
}

func (a postgresAnalysis) PageEvidence(ctx context.Context, document, hash,
	version, model string, pages int64) ([]database.EvidenceRow, error) {
	rows, err := a.db.Query(ctx, `SELECT page_number,`+
		`CASE WHEN octet_length(payload_json)<=262144 THEN payload_json END`+
		` FROM knowledge.page_enrichments WHERE document_id=$1 AND source_hash=$2`+
		` AND analysis_version=$3 AND requested_model=$4 AND status='ready'`+
		` AND page_number BETWEEN 1 AND $5 ORDER BY page_number`,
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

func (a postgresAnalysis) PublishReady(ctx context.Context,
	params database.AnalysisPublishParams) error {
	if params.Page > 0 {
		_, err := a.db.Exec(ctx, `UPDATE knowledge.page_enrichments`+
			` SET status='ready',model=$4,payload_json=$5,generated_at=$6,error=NULL,claimed_at=NULL`+
			` WHERE document_id=$1 AND page_number=$2 AND claimed_at=$3 AND status='running'`+
			` AND source_hash=$7 AND requested_model=$8 AND analysis_version=$9`,
			params.DocumentID, params.Page, params.ClaimedAt, params.Model,
			params.Payload, params.GeneratedAt, params.Hash, params.Requested, params.Version)
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE knowledge.document_enrichments`+
		` SET status='ready',model=$3,requested_model=$4,payload_json=$5,generated_at=$6,`+
		`error=NULL,claimed_at=NULL WHERE document_id=$1 AND claimed_at=$2 AND status='running'`+
		` AND source_hash=$7 AND context_hash=$8 AND analysis_version=$9`,
		params.DocumentID, params.ClaimedAt, params.Model, params.Requested,
		params.Payload, params.GeneratedAt, params.Hash, params.ContextHash, params.Version)
	return err
}

func (a postgresAnalysis) FinishClaim(ctx context.Context, params database.AnalysisFinishParams) error {
	if params.Page > 0 {
		_, err := a.db.Exec(ctx, `UPDATE knowledge.page_enrichments`+
			` SET status=$4,error=$5,available_at=$6,claimed_at=NULL,`+
			`attempts=CASE WHEN $7 THEN greatest(0,attempts-1) ELSE attempts END`+
			` WHERE document_id=$1 AND page_number=$2 AND claimed_at=$3 AND status='running'`,
			params.DocumentID, params.Page, params.ClaimedAt, params.Status,
			params.Error, params.AvailableAt, params.Reset)
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE knowledge.document_enrichments`+
		` SET status=$3,error=$4,available_at=$5,claimed_at=NULL,`+
		`attempts=CASE WHEN $6 THEN greatest(0,attempts-1) ELSE attempts END`+
		` WHERE document_id=$1 AND claimed_at=$2 AND status='running'`,
		params.DocumentID, params.ClaimedAt, params.Status,
		params.Error, params.AvailableAt, params.Reset)
	return err
}

func (a postgresAnalysis) RecoverLane(ctx context.Context, availableAt string) error {
	if _, err := a.db.Exec(ctx, `UPDATE knowledge.document_enrichments`+
		` SET status='pending',claimed_at=NULL,attempts=greatest(0,attempts-1),`+
		`available_at=$1 WHERE status='running'`, availableAt); err != nil {
		return err
	}
	_, err := a.db.Exec(ctx, `UPDATE knowledge.page_enrichments`+
		` SET status='pending',claimed_at=NULL,attempts=greatest(0,attempts-1),`+
		`available_at=$1 WHERE status='running'`, availableAt)
	return err
}

func scanExcerpts(rows nativeRows) ([]database.ExcerptRow, error) {
	out := []database.ExcerptRow{}
	for rows.Next() {
		var item database.ExcerptRow
		if err := rows.Scan(&item.LocatorType, &item.LocatorStart, &item.Text); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
