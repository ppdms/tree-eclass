package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func resourceDocument(ctx context.Context, tx database.Tx, id string) (database.KnowledgeDocument, error) {
	return tx.Documents().GetReadableDocument(ctx, id)
}

func (s Reader) Document(ctx context.Context, id string) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	doc, err := resourceDocument(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	result := documentEvidence(doc)
	for key, value := range documentMetrics(doc) {
		result[key] = value
	}
	result["status"], result["untrusted_content_notice"] = doc.Status, UntrustedNotice
	return result, tx.Commit(ctx)
}

func documentMetrics(doc database.KnowledgeDocument) map[string]any {
	return map[string]any{
		"source_size_bytes": doc.SourceSizeBytes,
		"page_count":        doc.PageCount,
		"character_count":   doc.CharacterCount,
		"word_count":        doc.WordCount,
		"reading_minutes":   doc.ReadingMinutes,
		"complexity_score":  doc.ComplexityScore,
		"complexity_label":  doc.ComplexityLabel,
	}
}

func (s Reader) MaterialInsight(ctx context.Context, id string) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	doc, err := resourceDocument(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	analysis, err := readDocumentAnalysis(ctx, tx, a, id, doc.SourceHash)
	if err != nil {
		return nil, err
	}
	warnings := []string{}
	if len(doc.WarningsJson) <= 65536 {
		_ = json.Unmarshal([]byte(doc.WarningsJson), &warnings)
	}
	if warnings == nil {
		warnings = []string{}
	}
	metrics := documentMetrics(doc)
	metrics["warnings"] = warnings
	paths := []string{}
	insight, _ := analysis["insight"].(map[string]any)
	if raw, ok := insight["related_paths"].([]any); ok {
		for _, value := range raw[:min(100, len(raw))] {
			if text, ok := value.(string); ok {
				paths = append(paths, identity.Path(text))
			}
		}
	}
	related, err := relatedMaterials(ctx, tx, doc.CourseID, paths)
	if err != nil {
		return nil, err
	}
	coverage, err := visualCoverage(ctx, tx, doc, a)
	if err != nil {
		return nil, err
	}
	metadata := documentEvidence(doc)
	for key, value := range documentMetrics(doc) {
		metadata[key] = value
	}
	return map[string]any{
		"document":                 metadata,
		"deterministic_metadata":   metrics,
		"study_analysis":           analysis,
		"visual_analysis_coverage": coverage,
		"related_materials":        related,
		"source_resource_uri":      "eclass://documents/" + id,
		"derived_insight_notice":   DerivedNotice,
		"untrusted_content_notice": UntrustedNotice,
	}, tx.Commit(ctx)
}

func (s Reader) PageInsight(ctx context.Context, id string, page int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	doc, err := resourceDocument(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if (doc.DocumentKind != "pdf" && doc.DocumentKind != "image") || doc.PageCount == nil || page < 1 ||
		page > *doc.PageCount {
		return nil, errors.New("page is outside this visual document")
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	analysis := map[string]any{
		"status":                      "not_queued",
		"ready":                       false,
		"insight":                     map[string]any{},
		"derived_not_source_evidence": true,
		"untrusted_content":           true,
	}
	row, err := tx.Documents().PageAnalysis(ctx, database.PageAnalysisParams{
		Document: id, Page: page, Hash: doc.SourceHash,
		Version: settings.PageAnalysisVersion, Model: a.Model, MaxBytes: 4194304,
	})
	if err != nil && !errors.Is(err, database.ErrNoRows) {
		return nil, err
	}
	if err == nil {
		analysis["status"], analysis["model"], analysis["generated_at"], analysis["analysis_version"] =
			row.Status, row.Model, row.GeneratedAt, settings.PageAnalysisVersion
		if row.Status == "ready" && row.Payload != nil {
			insight := pagePayload(*row.Payload)
			analysis["insight"], analysis["ready"] = insight, insight["summary"] != nil
		}
	}
	return map[string]any{
		"document":                 documentEvidence(doc),
		"page_number":              page,
		"page_count":               doc.PageCount,
		"page_analysis":            analysis,
		"source_resource_uri":      "eclass://documents/" + id + "/units/page:" + strconv.FormatInt(page, 10),
		"derived_insight_notice":   PageNotice,
		"untrusted_content_notice": UntrustedNotice,
	}, tx.Commit(ctx)
}

func visualCoverage(
	ctx context.Context,
	tx database.Tx,
	doc database.KnowledgeDocument,
	a settings.AI,
) (map[string]any, error) {
	rows, err := tx.Documents().PageCoverage(ctx, database.PageCoverageParams{
		Document: doc.ID, Hash: doc.SourceHash, Version: settings.PageAnalysisVersion, Model: a.Model,
	})
	if err != nil {
		return nil, err
	}
	statuses := map[string]int64{}
	var total int64
	var model, version, uri any
	for _, row := range rows {
		statuses[row.Status] = row.Count
		total += row.Count
		model = row.Model
	}
	if total > 0 {
		version = settings.PageAnalysisVersion
		uri = "eclass://documents/" + doc.ID + "/pages/{page_number}/insight"
	}
	return map[string]any{
		"ready_pages":               statuses["ready"],
		"total_pages":               total,
		"complete":                  total > 0 && statuses["ready"] == total,
		"statuses":                  statuses,
		"model":                     model,
		"analysis_version":          version,
		"page_insight_uri_template": uri,
	}, nil
}
