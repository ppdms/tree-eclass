package knowledge

import (
	"context"
	"encoding/json"
	"strconv"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func (s Reader) Status(ctx context.Context, course *int64) (map[string]any, error) {
	var requested []int64
	if course != nil {
		requested = []int64{*course}
	}
	return s.StatusFor(ctx, requested)
}

func (s Reader) StatusFor(ctx context.Context, requested []int64) (map[string]any, error) {
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	coverage, err := tx.Documents().StatusCoverage(ctx, ids)
	if err != nil {
		return nil, err
	}
	result["coverage"] = coverageMaps(coverage)
	counts, err := tx.Documents().StatusCounts(ctx, ids)
	if err != nil {
		return nil, err
	}
	result["documents"] = statusCountMaps(counts)
	jobs, err := tx.Documents().IndexJobCounts(ctx, ids)
	if err != nil {
		return nil, err
	}
	result["jobs"] = statusCountMaps(jobs)
	failed, err := tx.Documents().FailedDocuments(ctx, ids, 501)
	if err != nil {
		return nil, err
	}
	result["failed_documents"] = diagnosticMaps(failed)
	unsupported, err := tx.Documents().UnsupportedDocuments(ctx, ids, 501)
	if err != nil {
		return nil, err
	}
	result["unsupported_documents"] = diagnosticMaps(unsupported)
	if err = guideStatus(ctx, tx, ids, a, result); err != nil {
		return nil, err
	}
	result["roadmap_diagnostics"], err = roadmapDiagnostics(ctx, tx, ids, a)
	if err != nil {
		return nil, err
	}
	result["ai_pipeline"] = map[string]any{
		"enabled":          a.EnrichmentEnabled,
		"model":            a.Model,
		"course_model":     a.CourseModel,
		"course_enabled":   a.CourseEnabled,
		"practice_enabled": a.PracticeEnabled,
	}
	result["embedding"], err = embeddingStatus(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	result["diagnostics_truncated"] = len(result["failed_documents"].([]map[string]any)) > 500 ||
		len(result["unsupported_documents"].([]map[string]any)) > 500
	for _, key := range []string{"failed_documents", "unsupported_documents"} {
		rows := result[key].([]map[string]any)
		result[key] = rows[:min(500, len(rows))]
	}
	return result, tx.Commit(ctx)
}

func roadmapDiagnostics(
	ctx context.Context, tx database.Tx, ids []int64, a settings.AI,
) ([]map[string]any, error) {
	roadmaps := []map[string]any{}
	for _, id := range ids {
		ready, err := Readiness(ctx, tx, id, a)
		if err != nil {
			return nil, err
		}
		status, err := tx.Documents().BlueprintStatus(ctx, id, a.CourseModel, settings.CourseAnalysisVersion)
		if err != nil {
			return nil, err
		}
		roadmaps = append(roadmaps, map[string]any{"course_id": id, "status": status, "readiness": ready})
	}
	return roadmaps, nil
}

// coverageMaps renders coverage rows in the previous JSON shape: counts and
// ids as json.Number, matching the old to_jsonb transport.
func coverageMaps(rows []database.CoverageRow) []map[string]any {
	out := []map[string]any{}
	for _, row := range rows {
		out = append(out, map[string]any{
			"course_id":           json.Number(strconv.FormatInt(row.CourseID, 10)),
			"supported_documents": json.Number(strconv.FormatInt(row.Supported, 10)),
			"indexed_documents":   json.Number(strconv.FormatInt(row.Indexed, 10)),
			"failed_documents":    json.Number(strconv.FormatInt(row.Failed, 10)),
			"pending_documents":   json.Number(strconv.FormatInt(row.Pending, 10)),
		})
	}
	return out
}

func statusCountMaps(rows []database.StatusCount) []map[string]any {
	out := []map[string]any{}
	for _, row := range rows {
		out = append(out, map[string]any{
			"status": row.Status,
			"count":  json.Number(strconv.FormatInt(row.Count, 10)),
		})
	}
	return out
}

func diagnosticMaps(rows []database.DiagnosticDocument) []map[string]any {
	out := []map[string]any{}
	for _, row := range rows {
		item := map[string]any{
			"document_id":       row.DocumentID,
			"course_id":         json.Number(strconv.FormatInt(row.CourseID, 10)),
			"display_name":      identity.Decode(row.Display),
			"source_path":       identity.Decode(row.Path),
			"status":            row.Status,
			"diagnostic_reason": row.Reason,
			"error":             row.Error,
		}
		if row.Reason != nil {
			item["diagnostic_reason"] = identity.Decode(*row.Reason)
		}
		if row.Error != nil {
			item["error"] = identity.Decode(*row.Error)
		}
		out = append(out, item)
	}
	return out
}

func embeddingStatus(ctx context.Context, tx database.Tx, ids []int64) (map[string]any, error) {
	chunks, embedded, err := tx.Documents().EmbeddingCounts(ctx, ids, LocalEmbeddingModel)
	return map[string]any{
		"model":           LocalEmbeddingModel,
		"dimensions":      EmbeddingDimensions,
		"chunks":          chunks,
		"embedded_chunks": embedded,
		"missing_chunks":  chunks - embedded,
	}, err
}

func guideStatus(ctx context.Context, tx database.Tx, ids []int64, a settings.AI, result map[string]any) error {
	params := database.GuideFreshnessParams{
		Courses: ids, Model: a.Model, DocumentVersion: settings.DocumentAnalysisVersion,
		SynthesisVersion: settings.PageSynthesisVersion,
	}
	counts, err := tx.Documents().GuideSummary(ctx, params)
	if err != nil {
		return err
	}
	rows, err := tx.Documents().GuideDiagnostics(ctx, params, 501)
	if err != nil {
		return err
	}
	diagnostics := []map[string]any{}
	for _, row := range rows {
		item := map[string]any{
			"document_id":  row.DocumentID,
			"course_id":    json.Number(strconv.FormatInt(row.CourseID, 10)),
			"display_name": identity.Decode(row.Display),
			"source_path":  identity.Decode(row.Path),
			"model":        row.Model,
			"error":        row.Error,
			"status":       row.Status,
			"reason":       row.Reason,
		}
		if row.Error != nil {
			item["error"] = identity.Decode(*row.Error)
		}
		diagnostics = append(diagnostics, item)
	}
	result["guide_summary"], result["guide_diagnostics"], result["guide_diagnostics_truncated"] =
		statusCountMaps(counts), diagnostics[:min(500, len(diagnostics))], len(diagnostics) > 500
	return nil
}
