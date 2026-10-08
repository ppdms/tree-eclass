package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

// Readiness aggregates counts in the database. It never loads document bodies
// or every page insight into the API/processor's heap.
func Readiness(ctx context.Context, tx database.Operations, course int64, a settings.AI) (map[string]any, error) {
	counts := map[string]map[string]int64{
		"documents":            {},
		"extraction_jobs":      {},
		"document_enrichments": {},
		"page_enrichments":     {},
	}
	rows, err := tx.Documents().ReadinessCounts(ctx, database.ReadinessParams{
		Course: course, Model: a.Model, DocumentVersion: settings.DocumentAnalysisVersion,
		SynthesisVersion: settings.PageSynthesisVersion, PageVersion: settings.PageAnalysisVersion,
	})
	if err != nil {
		return nil, err
	}
	var ready, unverified, missing int64
	for _, row := range rows {
		switch row.Category {
		case "ready":
			ready = row.Count
		case "unverified":
			unverified = row.Count
		case "missing":
			missing = row.Count
		default:
			counts[row.Category][row.Status] = row.Count
		}
	}
	return readinessSummary(course, counts, ready, unverified, missing), nil
}

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
		{active("documents"), "documents_pending_extraction"},
		{active("extraction_jobs"), "extraction_jobs_active"},
		{active("document_enrichments"), "document_enrichments_active"},
		{active("page_enrichments"), "page_enrichments_active"},
		{missing, "document_enrichments_missing_current_generation"},
		{unverified, "documents_unverified_content"},
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
