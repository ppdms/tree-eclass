package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// postgresAnalysis implements database.Analysis with native PostgreSQL SQL:
// $N placeholders, schema-qualified tables, strpos admission and
// clock_timestamp eligibility evaluated at the backend clock.

func (a postgresAnalysis) FindCandidate(ctx context.Context, model string,
	versions database.AnalysisVersions) (database.AnalysisClaim, error) {
	var out database.AnalysisClaim
	err := a.db.QueryRow(ctx, `SELECT d.id,d.course_id FROM knowledge.documents d`+
		` JOIN app.courses c ON c.id=d.course_id`+
		` LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id`+
		` WHERE `+analysisEligiblePG+
		` AND (e.document_id IS NULL OR e.source_hash<>d.source_hash OR e.context_hash<>`+analysisContextPG+
		` OR coalesce(e.requested_model,e.model)<>$1`+
		` OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $3 ELSE $2 END`+
		` OR (e.status='pending' AND `+analysisDuePG("e.available_at")+` AND `+
		analysisVisualDuePG(1, 4)+`))`+
		` ORDER BY coalesce(e.priority,0) DESC,e.available_at NULLS FIRST,d.indexed_at,d.id LIMIT 1`,
		model, versions.Document, versions.Synthesis, versions.Page).
		Scan(&out.DocumentID, &out.CourseID)
	return out, err
}

func (a postgresAnalysis) LockCourseForClaim(ctx context.Context, course int64) error {
	var locked int64
	return a.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, course).
		Scan(&locked)
}

func (a postgresAnalysis) CurrentDocument(ctx context.Context, id string,
	lock bool) (database.AnalysisDocument, error) {
	var out database.AnalysisDocument
	query := `SELECT d.id,d.course_id,d.source_hash,d.document_kind,d.display_name,c.name,` +
		`d.normalized_path,d.source_origin,coalesce(d.page_count,0)` +
		` FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id` +
		` WHERE d.id=$1 AND ` + analysisEligiblePG
	if lock {
		query += " FOR UPDATE OF d"
	}
	err := a.db.QueryRow(ctx, query, id).Scan(
		&out.ID, &out.CourseID, &out.SourceHash, &out.Kind, &out.Name,
		&out.CourseName, &out.Path, &out.Origin, &out.Pages)
	if err != nil {
		return database.AnalysisDocument{}, err
	}
	return out, nil
}

func (a postgresAnalysis) QueueDocument(ctx context.Context,
	params database.QueueDocumentParams) error {
	_, err := a.db.Exec(ctx, `INSERT INTO knowledge.document_enrichments`+
		`(document_id,source_hash,context_hash,analysis_version,status,model,requested_model,available_at)`+
		` VALUES($1,$2,$3,$4,'pending',$5,$5,$6) ON CONFLICT(document_id) DO UPDATE`+
		` SET source_hash=excluded.source_hash,context_hash=excluded.context_hash,`+
		`analysis_version=excluded.analysis_version,model=excluded.model,`+
		`requested_model=excluded.requested_model,status='pending',payload_json=NULL,`+
		`attempts=0,available_at=excluded.available_at,claimed_at=NULL,error=NULL`+
		` WHERE knowledge.document_enrichments.source_hash<>excluded.source_hash`+
		` OR knowledge.document_enrichments.context_hash<>excluded.context_hash`+
		` OR knowledge.document_enrichments.analysis_version<>excluded.analysis_version`+
		` OR coalesce(knowledge.document_enrichments.requested_model,`+
		`knowledge.document_enrichments.model)<>excluded.requested_model`,
		params.DocumentID, params.SourceHash, params.ContextHash, params.Version,
		params.Model, params.AvailableAt)
	return err
}

func (a postgresAnalysis) ClaimDocument(ctx context.Context, document,
	claimedAt string) (int64, error) {
	var attempts int64
	err := a.db.QueryRow(ctx, `UPDATE knowledge.document_enrichments`+
		` SET status='running',claimed_at=$2,attempts=attempts+1 WHERE document_id=$1`+
		` AND status='pending' AND `+analysisDuePG("available_at")+` RETURNING attempts`,
		document, claimedAt).Scan(&attempts)
	return attempts, err
}

func (a postgresAnalysis) FailDocumentPageRange(ctx context.Context, document string) error {
	_, err := a.db.Exec(ctx, `UPDATE knowledge.document_enrichments SET status='failed',`+
		`error='Visual document page count is outside analysis limits (1-12000).'`+
		` WHERE document_id=$1`, document)
	return err
}

func (a postgresAnalysis) ClaimPage(ctx context.Context, document,
	claimedAt string) (database.PageClaim, error) {
	var out database.PageClaim
	err := a.db.QueryRow(ctx, `UPDATE knowledge.page_enrichments`+
		` SET status='running',claimed_at=$2,attempts=attempts+1 WHERE document_id=$1`+
		` AND page_number=(SELECT page_number FROM knowledge.page_enrichments`+
		` WHERE document_id=$1 AND status='pending' AND `+analysisDuePG("available_at")+
		` ORDER BY priority DESC,page_number LIMIT 1) RETURNING page_number,attempts`,
		document, claimedAt).Scan(&out.Page, &out.Attempts)
	return out, err
}

func (a postgresAnalysis) ReadyPageCount(ctx context.Context, document, hash,
	model, version string, pages int64) (int64, error) {
	var ready int64
	err := a.db.QueryRow(ctx, `SELECT count(*) FROM knowledge.page_enrichments`+
		` WHERE document_id=$1 AND status='ready' AND source_hash=$2 AND requested_model=$3`+
		` AND analysis_version=$4 AND page_number BETWEEN 1 AND $5`,
		document, hash, model, version, pages).Scan(&ready)
	return ready, err
}
