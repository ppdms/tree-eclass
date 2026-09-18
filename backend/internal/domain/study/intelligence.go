package study

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func buildIntelligence(
	ctx context.Context,
	tx pgx.Tx,
	selected *int64,
	a settings.AI,
	today time.Time,
) (map[string]any, error) {
	return buildIntelligenceSet(ctx, tx, selected, nil, a, today)
}

func buildIntelligenceSet(
	ctx context.Context,
	tx pgx.Tx,
	selected *int64,
	included []int64,
	a settings.AI,
	today time.Time,
) (map[string]any, error) {
	plans, err := settings.ReadExamPlans(ctx, tx)
	if err != nil {
		return nil, err
	}
	if included != nil {
		plans = slices.DeleteFunc(
			plans,
			func(p settings.ExamPlan) bool { return !slices.Contains(included, p.CourseID) },
		)
	}
	accumulator := newPriorities(plans, selected, today)
	rows, err := tx.Query(
		ctx,
		intelligenceQuery,
		a.Model,
		settings.DocumentAnalysisVersion,
		settings.PageSynthesisVersion,
		included,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if err := collectPriorities(rows, accumulator); err != nil {
		return nil, err
	}
	return accumulator.result(), nil
}

func collectPriorities(rows pgx.Rows, accumulator *priorities) error {
	for rows.Next() {
		m := &PriorityMaterial{AI: map[string]any{}}
		var raw *string
		if err := rows.Scan(
			&m.ID,
			&m.CourseID,
			&m.CourseName,
			&m.Name,
			&m.Path,
			&m.Origin,
			&m.Kind,
			&m.Complexity,
			&m.Reading,
			&m.Pages,
			&m.Words,
			&m.Level,
			&raw,
		); err != nil {
			return err
		}
		for _, value := range []*string{&m.CourseName, &m.Name, &m.Path} {
			*value = identity.Decode(*value)
		}
		if raw != nil {
			m.AI, m.Enriched = priorityInsight(*raw)
		}
		accumulator.Add(m)
	}
	return rows.Err()
}

const intelligenceQuery = `SELECT d.id,d.course_id,coalesce(nullif(c.short_name,''),c.name),d.display_name,d.source_path,d.source_origin,d.document_kind,
 coalesce(d.complexity_score,0),d.reading_minutes,d.page_count,d.word_count,coalesce(l.level,0),
 CASE WHEN e.status='ready' AND octet_length(e.payload_json)<=4194304 THEN e.payload_json END
 FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id
 LEFT JOIN app.file_study l ON l.course_id=d.course_id AND l.file_path=d.source_path
 LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=$1
 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $3 ELSE $2 END
 WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1))
 AND ($4::bigint[] IS NULL OR c.id=ANY($4::bigint[]))
 AND d.is_current=1 AND d.status='ready' AND d.content_hash_verified=1
 AND ((d.document_kind IN('pdf','image') AND coalesce(d.page_count,0)>0) OR EXISTS(SELECT 1 FROM knowledge.chunks chunk WHERE chunk.document_id=d.id AND length(trim(chunk.text))>0))
 AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.deleted_at IS NULL AND r.logical_path=d.normalized_path AND o.sha256=d.source_hash)
 ORDER BY d.course_id,d.normalized_path`

func priorityInsight(raw string) (map[string]any, bool) {
	var payload map[string]any
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return map[string]any{}, false
	}
	summary, _ := payload["summary"].(string)
	if strings.TrimSpace(summary) == "" {
		return map[string]any{}, false
	}
	result := map[string]any{}
	for _, key := range []string{"summary", "importance", "importance_reason", "difficulty", "assessment_relevance", "material_type", "recommended_action", "course_role"} {
		if text, ok := payload[key].(string); ok {
			// Priority cards contain an excerpt. The exact guide remains on its
			// own document endpoint rather than being copied into every schedule.
			runes := []rune(text)
			if len(runes) > 1200 {
				text = string(runes[:1199]) + "…"
			}
			result[key] = text
		}
	}
	return result, true
}
