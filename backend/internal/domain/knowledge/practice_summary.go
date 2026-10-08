package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

func PracticeSummary(
	ctx context.Context,
	tx database.Operations,
	course int64,
	revision string,
	units []string,
	a settings.AI,
) (map[string]any, error) {
	result := map[string]any{
		"enabled":              a.PracticeEnabled && a.CourseEnabled && a.EnrichmentEnabled,
		"units_with_questions": int64(0),
		"question_count":       int64(0),
		"pending_units":        int64(0),
		"failed_units":         int64(0),
	}
	counts := map[string]int64{}
	rows, err := tx.Practice().SetStatusCounts(ctx, database.PracticeSetSelector{
		CourseID:        course,
		Revision:        revision,
		AnalysisVersion: settings.PracticeAnalysisVersion,
		Model:           a.PracticeModel,
	}, units)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		counts[row.Status] = row.Sets
		if row.Status == "ready" {
			result["units_with_questions"], result["question_count"] = row.Sets, row.Questions
		}
	}
	result["set_counts"] = counts
	result["pending_units"], result["failed_units"] = counts["pending"]+counts["running"], counts["failed"]
	return result, nil
}
