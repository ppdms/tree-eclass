package study

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func buildIntelligence(
	ctx context.Context,
	ops database.Operations,
	selected *int64,
	a settings.AI,
	today time.Time,
) (map[string]any, error) {
	return buildIntelligenceSet(ctx, ops, selected, nil, a, today)
}

func buildIntelligenceSet(
	ctx context.Context,
	ops database.Operations,
	selected *int64,
	included []int64,
	a settings.AI,
	today time.Time,
) (map[string]any, error) {
	plans, err := settings.ReadExamPlans(ctx, ops)
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
	items, err := ops.Study().ListPriorityMaterials(ctx, database.StudyIntelligenceParams{
		Model:           a.Model,
		DocumentVersion: settings.DocumentAnalysisVersion,
		PageVersion:     settings.PageSynthesisVersion,
		Included:        included,
	})
	if err != nil {
		return nil, err
	}
	defer items.Close()
	for items.Next() {
		accumulator.Add(priorityMaterial(items.Value()))
	}
	if err := items.Err(); err != nil {
		return nil, err
	}
	return accumulator.result(), nil
}

func priorityMaterial(row database.StudyPriorityMaterial) *PriorityMaterial {
	m := &PriorityMaterial{
		ID: row.ID, CourseID: row.CourseID,
		CourseName: identity.Decode(row.CourseName), Name: identity.Decode(row.Name),
		Path: identity.Decode(row.Path), Origin: row.Origin, Kind: row.Kind,
		Complexity: row.Complexity, Reading: row.Reading, Pages: row.Pages,
		Words: row.Words, Level: row.Level, AI: map[string]any{},
	}
	if row.Enrichment != nil {
		m.AI, m.Enriched = priorityInsight(*row.Enrichment)
	}
	return m
}

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
	keys := []string{"summary", "importance", "importance_reason", "difficulty",
		"assessment_relevance", "material_type", "recommended_action", "course_role"}
	for _, key := range keys {
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
