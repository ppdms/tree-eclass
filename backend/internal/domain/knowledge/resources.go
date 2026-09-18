package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func resourceDocument(ctx context.Context, tx pgx.Tx, id string) (queries.KnowledgeDocument, error) {
	var found string
	err := tx.QueryRow(ctx, `SELECT d.id FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id AND c.hidden=0 WHERE d.id=$1 AND `+CurrentSourcePredicate, id).
		Scan(&found)
	if err != nil {
		return queries.KnowledgeDocument{}, err
	}
	return queries.New(tx).IndexDocument(ctx, id)
}

func (s Reader) Document(ctx context.Context, id string) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
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

func documentMetrics(doc queries.KnowledgeDocument) map[string]any {
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
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
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
	}, tx.Commit(
		ctx,
	)
}

func (s Reader) PageInsight(ctx context.Context, id string, page int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
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
	var status, model string
	var raw, generated *string
	err = tx.QueryRow(ctx, `SELECT status,model,CASE WHEN octet_length(payload_json)<=4194304 THEN payload_json END,generated_at FROM knowledge.page_enrichments WHERE document_id=$1 AND page_number=$2 AND source_hash=$3 AND analysis_version=$4 AND requested_model=$5`, id, page, doc.SourceHash, settings.PageAnalysisVersion, a.Model).
		Scan(&status, &model, &raw, &generated)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil {
		analysis["status"], analysis["model"], analysis["generated_at"], analysis["analysis_version"] = status, model, generated, settings.PageAnalysisVersion
		if status == "ready" && raw != nil {
			insight := pagePayload(*raw)
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
	}, tx.Commit(
		ctx,
	)
}

func visualCoverage(
	ctx context.Context,
	tx pgx.Tx,
	doc queries.KnowledgeDocument,
	a settings.AI,
) (map[string]any, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT status,count(*),min(model) FROM knowledge.page_enrichments WHERE document_id=$1 AND source_hash=$2 AND analysis_version=$3 AND requested_model=$4 GROUP BY status`,
		doc.ID,
		doc.SourceHash,
		settings.PageAnalysisVersion,
		a.Model,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	statuses := map[string]int64{}
	var total int64
	var model, version, uri any
	for rows.Next() {
		var status, name string
		var count int64
		if err = rows.Scan(&status, &count, &name); err != nil {
			return nil, err
		}
		statuses[status] = count
		total += count
		model = name
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
	}, rows.Err()
}
