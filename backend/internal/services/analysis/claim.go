package analysis

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
)

const eligible = knowledge.CurrentSourcePredicate + ` AND d.status='ready' AND d.content_hash_verified=1 AND d.document_kind<>'archive'
 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans plan WHERE plan.course_id=c.id AND plan.enabled=1))`

const documentContext = `encode(sha256(convert_to(jsonb_build_array(d.id,d.course_id,d.source_hash,d.document_kind,d.display_name,c.name,d.normalized_path,d.source_origin)::text,'UTF8')),'hex')`

const visualDue = `(d.document_kind NOT IN('pdf','image') OR NOT EXISTS(SELECT 1 FROM knowledge.page_enrichments p WHERE p.document_id=d.id)
 OR EXISTS(SELECT 1 FROM knowledge.page_enrichments p WHERE p.document_id=d.id AND (p.source_hash<>d.source_hash OR p.requested_model<>$1 OR p.analysis_version<>$4 OR (p.status='pending' AND p.available_at::timestamptz<=clock_timestamp())))
 OR (SELECT count(*) FROM knowledge.page_enrichments p WHERE p.document_id=d.id AND p.status='ready' AND p.source_hash=d.source_hash AND p.requested_model=$1 AND p.analysis_version=$4 AND p.page_number BETWEEN 1 AND d.page_count)=d.page_count)`

const due = `(CASE WHEN pg_input_is_valid(e.available_at,'timestamptz') THEN e.available_at::timestamptz ELSE '-infinity'::timestamptz END)<=clock_timestamp()`

func (s Service) claim(ctx context.Context) (job, error) {
	var j job
	a, err := settings.ReadAI(ctx, s.Pool)
	if err != nil {
		return j, err
	}
	if !a.EnrichmentEnabled {
		return j, pgx.ErrNoRows
	}
	// Inspect candidates without holding a lock across I/O. Recheck under the
	// course/document locks, then commit the claim before calling a provider.
	id, course, err := s.findCandidate(ctx, a.Model)
	if err != nil {
		return j, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return j, err
	}
	defer tx.Rollback(ctx)
	d, a, err := s.recheckEligibility(ctx, tx, course, id)
	if err != nil {
		return j, err
	}
	j = job{
		Document:  d,
		Requested: a.Model,
		Version:   settings.DocumentVersion(d.Kind),
		AI:        a,
		Claim:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err = queueDocument(ctx, tx, j); err != nil {
		return j, err
	}
	if handled, err := s.claimVisual(ctx, tx, &j, d); handled || err != nil {
		return j, err
	}
	err = tx.QueryRow(ctx, `UPDATE knowledge.document_enrichments SET status='running',claimed_at=$2,attempts=attempts+1 WHERE document_id=$1 AND status='pending' AND (CASE WHEN pg_input_is_valid(available_at,'timestamptz') THEN available_at::timestamptz ELSE '-infinity'::timestamptz END)<=clock_timestamp() RETURNING attempts`, id, j.Claim).
		Scan(&j.Attempts)
	if err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s Service) findCandidate(ctx context.Context, model string) (string, int64, error) {
	var id string
	var course int64
	err := s.Pool.QueryRow(ctx, `SELECT d.id,d.course_id FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id WHERE `+eligible+`
 AND (e.document_id IS NULL OR e.source_hash<>d.source_hash OR e.context_hash<>`+documentContext+` OR coalesce(e.requested_model,e.model)<>$1 OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $3 ELSE $2 END OR (e.status='pending' AND `+due+` AND `+visualDue+`))
 ORDER BY coalesce(e.priority,0) DESC,e.available_at NULLS FIRST,d.indexed_at,d.id LIMIT 1`, model, settings.DocumentAnalysisVersion, settings.PageSynthesisVersion, settings.PageAnalysisVersion).
		Scan(&id, &course)
	return id, course, err
}
func (s Service) recheckEligibility(
	ctx context.Context,
	tx pgx.Tx,
	course int64,
	id string,
) (document, settings.AI, error) {
	var d document
	if err := tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, course).Scan(&course); err != nil {
		return d, settings.AI{}, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return d, a, err
	}
	if !a.EnrichmentEnabled {
		return d, a, pgx.ErrNoRows
	}
	d, err = currentDocument(ctx, tx, id, true)
	return d, a, err
}
func (s Service) claimVisual(ctx context.Context, tx pgx.Tx, j *job, d document) (bool, error) {
	if d.Kind != "pdf" && d.Kind != "image" {
		return false, nil
	}
	if d.Pages < 1 || d.Pages > 12000 {
		if _, err := tx.Exec(ctx, `UPDATE knowledge.document_enrichments SET status='failed',error='Visual document page count is outside analysis limits (1–12000).' WHERE document_id=$1`, d.ID); err != nil {
			return true, err
		}
		if err := tx.Commit(ctx); err != nil {
			return true, err
		}
		return true, pgx.ErrNoRows
	}
	page, attempts, err := claimPage(ctx, tx, *j)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return true, commitErr
		}
		return true, err
	}
	if err != nil {
		return true, err
	}
	j.Page, j.Attempts = page, attempts
	if page > 0 {
		j.Version = settings.PageAnalysisVersion
		return true, tx.Commit(ctx)
	}
	return false, nil
}
func currentDocument(ctx context.Context, tx pgx.Tx, id string, lock bool) (document, error) {
	var d document
	query := `SELECT d.id,d.course_id,d.source_hash,d.document_kind,d.display_name,c.name,d.normalized_path,d.source_origin,coalesce(d.page_count,0),` + documentContext + ` FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id WHERE d.id=$1 AND ` + eligible
	if lock {
		query += " FOR UPDATE OF d"
	}
	err := tx.QueryRow(ctx, query, id).
		Scan(&d.ID, &d.Course, &d.Hash, &d.Kind, &d.Name, &d.CourseName, &d.Path, &d.Origin, &d.Pages, &d.Context)
	return d, err
}
func queueDocument(ctx context.Context, tx pgx.Tx, j job) error {
	_, err := tx.Exec(
		ctx,
		`INSERT INTO knowledge.document_enrichments(document_id,source_hash,context_hash,analysis_version,status,model,requested_model,available_at) VALUES($1,$2,$3,$4,'pending',$5,$5,$6)
 ON CONFLICT(document_id) DO UPDATE SET source_hash=excluded.source_hash,context_hash=excluded.context_hash,analysis_version=excluded.analysis_version,model=excluded.model,requested_model=excluded.requested_model,status='pending',payload_json=NULL,attempts=0,available_at=excluded.available_at,claimed_at=NULL,error=NULL
 WHERE document_enrichments.source_hash<>excluded.source_hash OR document_enrichments.context_hash<>excluded.context_hash OR document_enrichments.analysis_version<>excluded.analysis_version OR coalesce(document_enrichments.requested_model,document_enrichments.model)<>excluded.requested_model`,
		j.Document.ID,
		j.Document.Hash,
		j.Document.Context,
		j.Version,
		j.Requested,
		j.Claim,
	)
	return err
}
func claimPage(ctx context.Context, tx pgx.Tx, j job) (int64, int64, error) {
	_, err := tx.Exec(
		ctx,
		`INSERT INTO knowledge.page_enrichments(document_id,page_number,source_hash,analysis_version,status,model,requested_model,available_at)
 SELECT $1,page,$2,$3,'pending',$4,$4,$5 FROM generate_series(1,$6::bigint) page
 ON CONFLICT(document_id,page_number) DO UPDATE SET source_hash=excluded.source_hash,analysis_version=excluded.analysis_version,status='pending',model=excluded.model,requested_model=excluded.requested_model,available_at=excluded.available_at,attempts=0,claimed_at=NULL,payload_json=NULL,error=NULL
 WHERE page_enrichments.source_hash<>excluded.source_hash OR page_enrichments.analysis_version<>excluded.analysis_version OR page_enrichments.requested_model<>excluded.requested_model`,
		j.Document.ID,
		j.Document.Hash,
		settings.PageAnalysisVersion,
		j.Requested,
		j.Claim,
		j.Document.Pages,
	)
	if err != nil {
		return 0, 0, err
	}
	var page, attempts int64
	err = tx.QueryRow(ctx, `UPDATE knowledge.page_enrichments SET status='running',claimed_at=$2,attempts=attempts+1 WHERE document_id=$1 AND page_number=(SELECT page_number FROM knowledge.page_enrichments WHERE document_id=$1 AND status='pending' AND available_at::timestamptz<=clock_timestamp() ORDER BY priority DESC,page_number LIMIT 1) RETURNING page_number,attempts`, j.Document.ID, j.Claim).
		Scan(&page, &attempts)
	if err == nil {
		return page, attempts, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}
	var ready int64
	err = tx.QueryRow(ctx, `SELECT count(*) FROM knowledge.page_enrichments WHERE document_id=$1 AND status='ready' AND source_hash=$2 AND requested_model=$3 AND analysis_version=$4 AND page_number BETWEEN 1 AND $5`, j.Document.ID, j.Document.Hash, j.Requested, settings.PageAnalysisVersion, j.Document.Pages).
		Scan(&ready)
	if err != nil {
		return 0, 0, err
	}
	if ready != j.Document.Pages {
		return 0, 0, pgx.ErrNoRows
	}
	return 0, 0, nil
}
