package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// sqliteAnalysis implements database.Analysis with native SQLite SQL: ?
// placeholders, unqualified table names, instr admission, julianday clocks
// and JSON1 length checks. No PostgreSQL compatibility functions remain.
// FindCandidate scans ordered eligible rows and evaluates due state in Go.

func (a sqliteAnalysis) FindCandidate(ctx context.Context, model string,
	versions database.AnalysisVersions) (database.AnalysisClaim, error) {
	rows, err := a.db.Query(ctx, `SELECT d.id,d.course_id,d.source_hash,d.document_kind,`+
		`d.display_name,c.name,d.normalized_path,d.source_origin,d.page_count,d.indexed_at,`+
		`e.document_id,e.source_hash,e.context_hash,e.requested_model,e.model,`+
		`e.analysis_version,e.status,e.available_at,e.priority FROM documents d`+
		` JOIN courses c ON c.id=d.course_id`+
		` LEFT JOIN document_enrichments e ON e.document_id=d.id`+
		` WHERE `+analysisEligibleSQLite+
		` ORDER BY coalesce(e.priority,0) DESC,e.available_at,d.indexed_at,d.id`)
	if err != nil {
		return database.AnalysisClaim{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var row analysisCandidateRow
		if err := rows.Scan(analysisCandidatePointers(&row)...); err != nil {
			return database.AnalysisClaim{}, err
		}
		if !row.staleInputs(model, versions) {
			if !row.pending || !row.pendingDue {
				continue
			}
			due, err := a.visualDue(ctx, row.id, row.hash, row.kind, model, versions.Page, row.pages)
			if err != nil || !due {
				if err != nil {
					return database.AnalysisClaim{}, err
				}
				continue
			}
		}
		return database.AnalysisClaim{DocumentID: row.id, CourseID: row.course}, nil
	}
	if err := rows.Err(); err != nil {
		return database.AnalysisClaim{}, err
	}
	return database.AnalysisClaim{}, database.ErrNoRows
}

// visualDue reports whether a document still needs page work: non-visual
// kinds are always due, otherwise any missing, stale, or due pending page
// row makes it due, as does a complete ready page set.
func (a sqliteAnalysis) visualDue(ctx context.Context, document, hash, kind,
	model, version string, pages *int64) (bool, error) {
	if kind != "pdf" && kind != "image" {
		return true, nil
	}
	var missing bool
	err := a.db.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM page_enrichments`+
		` WHERE document_id=?)`, document).Scan(&missing)
	if err != nil || missing {
		return missing, err
	}
	var stale bool
	err = a.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM page_enrichments`+
		` WHERE document_id=? AND (source_hash<>? OR requested_model<>?`+
		` OR analysis_version<>? OR (status='pending' AND `+analysisDueSQLite("available_at")+`)))`,
		document, hash, model, version).Scan(&stale)
	if err != nil || stale {
		return stale, err
	}
	count, err := a.ReadyPageCount(ctx, document, hash, model, version, coalescePages(pages))
	return err == nil && count == coalescePages(pages), err
}

func coalescePages(pages *int64) int64 {
	if pages == nil {
		return 0
	}
	return *pages
}

func (a sqliteAnalysis) LockCourseForClaim(ctx context.Context, course int64) error {
	// The admitted writer owns every write until commit/rollback, so the
	// existence read is the lock.
	var locked int64
	return a.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`, course).Scan(&locked)
}

func (a sqliteAnalysis) CurrentDocument(ctx context.Context, id string,
	_ bool) (database.AnalysisDocument, error) {
	var out database.AnalysisDocument
	err := a.db.QueryRow(ctx, `SELECT d.id,d.course_id,d.source_hash,d.document_kind,`+
		`d.display_name,c.name,d.normalized_path,d.source_origin,coalesce(d.page_count,0)`+
		` FROM documents d JOIN courses c ON c.id=d.course_id`+
		` WHERE d.id=? AND `+analysisEligibleSQLite, id).Scan(
		&out.ID, &out.CourseID, &out.SourceHash, &out.Kind, &out.Name,
		&out.CourseName, &out.Path, &out.Origin, &out.Pages)
	if err != nil {
		return database.AnalysisDocument{}, err
	}
	return out, nil
}

func (a sqliteAnalysis) QueueDocument(ctx context.Context,
	params database.QueueDocumentParams) error {
	_, err := a.db.Exec(ctx, `INSERT INTO document_enrichments`+
		`(document_id,source_hash,context_hash,analysis_version,status,model,requested_model,available_at)`+
		` VALUES(?,?,?,?,'pending',?,?,?) ON CONFLICT(document_id) DO UPDATE`+
		` SET source_hash=excluded.source_hash,context_hash=excluded.context_hash,`+
		`analysis_version=excluded.analysis_version,model=excluded.model,`+
		`requested_model=excluded.requested_model,status='pending',payload_json=NULL,`+
		`attempts=0,available_at=excluded.available_at,claimed_at=NULL,error=NULL`+
		` WHERE document_enrichments.source_hash<>excluded.source_hash`+
		` OR document_enrichments.context_hash<>excluded.context_hash`+
		` OR document_enrichments.analysis_version<>excluded.analysis_version`+
		` OR coalesce(document_enrichments.requested_model,`+
		`document_enrichments.model)<>excluded.requested_model`,
		params.DocumentID, params.SourceHash, params.ContextHash, params.Version,
		params.Model, params.Model, params.AvailableAt)
	return err
}

func (a sqliteAnalysis) ClaimDocument(ctx context.Context, document,
	claimedAt string) (int64, error) {
	var attempts int64
	err := a.db.QueryRow(ctx, `UPDATE document_enrichments`+
		` SET status='running',claimed_at=?,attempts=attempts+1 WHERE document_id=?`+
		` AND status='pending' AND `+analysisDueSQLite("available_at")+` RETURNING attempts`,
		claimedAt, document).Scan(&attempts)
	return attempts, err
}

func (a sqliteAnalysis) FailDocumentPageRange(ctx context.Context, document string) error {
	_, err := a.db.Exec(ctx, `UPDATE document_enrichments SET status='failed',`+
		`error='Visual document page count is outside analysis limits (1-12000).'`+
		` WHERE document_id=?`, document)
	return err
}

func (a sqliteAnalysis) ClaimPage(ctx context.Context, document,
	claimedAt string) (database.PageClaim, error) {
	var out database.PageClaim
	err := a.db.QueryRow(ctx, `UPDATE page_enrichments`+
		` SET status='running',claimed_at=?,attempts=attempts+1 WHERE document_id=?`+
		` AND page_number=(SELECT page_number FROM page_enrichments`+
		` WHERE document_id=? AND status='pending' AND `+analysisDueSQLite("available_at")+
		` ORDER BY priority DESC,page_number LIMIT 1) RETURNING page_number,attempts`,
		claimedAt, document, document).Scan(&out.Page, &out.Attempts)
	return out, err
}

func (a sqliteAnalysis) ReadyPageCount(ctx context.Context, document, hash,
	model, version string, pages int64) (int64, error) {
	var ready int64
	err := a.db.QueryRow(ctx, `SELECT count(*) FROM page_enrichments`+
		` WHERE document_id=? AND status='ready' AND source_hash=? AND requested_model=?`+
		` AND analysis_version=? AND page_number BETWEEN 1 AND ?`,
		document, hash, model, version, pages).Scan(&ready)
	return ready, err
}
