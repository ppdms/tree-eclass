package knowledge

import (
	"context"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/settings"
)

func PracticeSummary(
	ctx context.Context,
	tx pgx.Tx,
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
	rows, err := tx.Query(ctx, `WITH chosen AS (
 SELECT DISTINCT ON(unit_key) id,status FROM knowledge.practice_question_sets
 WHERE course_id=$1 AND blueprint_revision_hash=$2 AND unit_key=ANY($3::text[])
 AND analysis_version=$4 AND requested_model=$5 AND status IN('ready','pending','running','failed')
 ORDER BY unit_key,(status='ready') DESC,id DESC
) SELECT status,count(*),coalesce(sum((SELECT count(*) FROM knowledge.practice_questions q WHERE q.set_id=s.id)),0)::bigint FROM chosen s GROUP BY status`, course, revision, units, settings.PracticeAnalysisVersion, a.PracticeModel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count, questions int64
		if err = rows.Scan(&status, &count, &questions); err != nil {
			return nil, err
		}
		counts[status] = count
		if status == "ready" {
			result["units_with_questions"], result["question_count"] = count, questions
		}
	}
	result["set_counts"], result["pending_units"], result["failed_units"] = counts, counts["pending"]+counts["running"], counts["failed"]
	return result, rows.Err()
}
