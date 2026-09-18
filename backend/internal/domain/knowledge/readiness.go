package knowledge

import (
	"context"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/settings"
)

// Readiness aggregates counts in PostgreSQL. It never loads document bodies or
// every page insight into the API/processor's heap.
func Readiness(ctx context.Context, tx pgx.Tx, course int64, a settings.AI) (map[string]any, error) {
	counts := map[string]map[string]int64{
		"documents":            {},
		"extraction_jobs":      {},
		"document_enrichments": {},
		"page_enrichments":     {},
	}
	rows, err := tx.Query(
		ctx,
		readinessQuery,
		course,
		a.Model,
		settings.DocumentAnalysisVersion,
		settings.PageSynthesisVersion,
		settings.PageAnalysisVersion,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ready, unverified, missing int64
	for rows.Next() {
		var category, status string
		var count int64
		if err = rows.Scan(&category, &status, &count); err != nil {
			return nil, err
		}
		switch category {
		case "ready":
			ready = count
		case "unverified":
			unverified = count
		case "missing":
			missing = count
		default:
			counts[category][status] = count
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return readinessSummary(course, counts, ready, unverified, missing), nil
}

const readinessQuery = `WITH docs AS MATERIALIZED (
 SELECT d.*,e.status enrichment_status, (` + CurrentSourcePredicate + `) source_admitted FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e
 ON e.document_id=d.id AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=$2
 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END
 WHERE d.course_id=$1 AND d.is_current=1
), evidence AS MATERIALIZED (SELECT * FROM docs WHERE status='ready' AND document_kind<>'archive' AND content_hash_verified=1 AND source_admitted),
 counts AS (
 SELECT 'documents' category,status,count(*) n FROM docs GROUP BY status
 UNION ALL SELECT 'extraction_jobs',c.status,count(*) FROM app.control_commands c JOIN docs d ON d.id=c.payload->>'document_id' WHERE c.queue='index' GROUP BY c.status
 UNION ALL SELECT 'document_enrichments',enrichment_status,count(*) FROM evidence WHERE enrichment_status IS NOT NULL GROUP BY enrichment_status
 UNION ALL SELECT 'page_enrichments',p.status,count(*) FROM knowledge.page_enrichments p JOIN evidence d ON d.id=p.document_id AND d.source_hash=p.source_hash WHERE p.analysis_version=$5 AND p.requested_model=$2 GROUP BY p.status
 UNION ALL SELECT 'ready','',count(*) FROM evidence
 UNION ALL SELECT 'unverified','',count(*) FROM docs WHERE status='ready' AND document_kind<>'archive' AND (content_hash_verified<>1 OR NOT source_admitted)
 UNION ALL SELECT 'missing','',count(*) FROM evidence WHERE enrichment_status IS NULL
) SELECT category,status,n FROM counts`

func readinessSummary(
	course int64,
	counts map[string]map[string]int64,
	ready, unverified, missing int64,
) map[string]any {
	active := func(category string) int64 { return counts[category]["pending"] + counts[category]["running"] }
	reasons := []string{}
	for _, item := range []struct {
		Count  int64
		Reason string
	}{
		{active("documents"), "documents_pending_extraction"}, {active("extraction_jobs"), "extraction_jobs_active"},
		{active("document_enrichments"), "document_enrichments_active"}, {active("page_enrichments"), "page_enrichments_active"},
		{missing, "document_enrichments_missing_current_generation"}, {unverified, "documents_unverified_content"},
	} {
		if item.Count > 0 {
			reasons = append(reasons, item.Reason)
		}
	}
	settled := len(reasons) == 0
	insights := counts["document_enrichments"]["ready"]
	if ready == 0 {
		reasons = append(reasons, "no_ready_documents")
	}
	if ready > 0 && insights == 0 && active("document_enrichments") == 0 {
		reasons = append(reasons, "no_ready_document_insights")
	}
	degraded := false
	for _, statuses := range counts {
		degraded = degraded || statuses["failed"] > 0
	}
	result := map[string]any{
		"course_id":                    course,
		"settled":                      settled,
		"ready":                        settled && ready > 0 && insights > 0,
		"degraded":                     degraded,
		"blocking_reasons":             reasons,
		"ready_documents":              ready,
		"ready_document_insights":      insights,
		"documents_without_enrichment": missing,
		"unverified_documents":         unverified,
		"active_extraction_jobs": active(
			"extraction_jobs",
		),
		"active_document_enrichments": active("document_enrichments"),
		"active_page_enrichments":     active("page_enrichments"),
	}
	for key, value := range counts {
		result[key] = value
		result["failed_"+key] = value["failed"]
	}
	return result
}
