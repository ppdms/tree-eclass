// Package analysis generates bounded, source-bound study guidance.
package analysis

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"tree-eclass/internal/domain/materials"
)

func text(value any, maximum int) string {
	var result string
	switch v := value.(type) {
	case string:
		result = v
	case json.Number:
		result = string(v)
	case float64:
		result = fmt.Sprint(v)
	}
	// Derived guidance has no meaningful NUL glyph. Source units retain their
	// exact identity separately; removing NUL here keeps JSONB diagnostics valid.
	chars := []rune(strings.TrimSpace(strings.ReplaceAll(result, "\x00", "")))
	return string(chars[:min(maximum, len(chars))])
}
func textList(value any, count, width int) []string {
	result := []string{}
	list, _ := value.([]any)
	for _, v := range list {
		item := text(v, width)
		if item != "" && !slices.Contains(result, item) {
			result = append(result, item)
		}
		if len(result) >= count {
			break
		}
	}
	return result
}
func choice(value any, maximum int, fallback string, allowed ...string) string {
	v := strings.ToLower(text(value, maximum))
	if slices.Contains(allowed, v) {
		return v
	}
	return fallback
}
func validateDocument(p map[string]any) (map[string]any, error) {
	if text(p["summary"], 1200) == "" {
		return nil, errors.New("model omitted document summary")
	}
	result := map[string]any{}
	for key, limit := range map[string]int{"summary": 1200, "course_role": 900, "importance_reason": 500, "difficulty_reason": 500, "course_alignment_reason": 600, "assessment_reason": 500, "recommended_action": 500} {
		result[key] = text(p[key], limit)
	}
	result["importance"] = choice(p["importance"], 24, "useful", "essential", "useful", "reference")
	result["difficulty"] = choice(p["difficulty"], 24, "intermediate", "introductory", "intermediate", "advanced")
	kind := choice(
		p["material_type"],
		32,
		"other",
		"lecture_notes",
		"slides",
		"tutorial",
		"assignment",
		"exercise_set",
		"worked_solutions",
		"past_exam",
		"practice_exam",
		"exam_guidance",
		"study_guide",
		"student_notes",
		"textbook",
		"results",
		"reference",
		"administrative",
		"code",
		"other",
	)
	result["material_type"] = kind
	fallback := materials.AnalysisType(kind)
	if fallback == "" {
		fallback = "other"
	}
	result["external_material_type"] = choice(
		p["external_material_type"],
		32,
		fallback,
		"past_paper",
		"student_notes",
		"study_guide",
		"textbook",
		"exercise_solution",
		"lecture_material",
		"other",
	)
	result["course_alignment"] = choice(p["course_alignment"], 24, "uncertain", "aligned", "uncertain", "mismatch")
	result["assessment_relevance"] = choice(
		p["assessment_relevance"],
		24,
		"unknown",
		"high",
		"medium",
		"low",
		"unknown",
	)
	for key, limits := range map[string][2]int{"topics": {8, 100}, "learning_objectives": {6, 180}, "prerequisites": {5, 120}, "transferable_concepts": {6, 100}, "visual_content": {6, 220}, "notable_items": {8, 240}} {
		result[key] = textList(p[key], limits[0], limits[1])
	}
	return result, nil
}
func validatePage(p map[string]any) (map[string]any, error) {
	summary := text(p["summary"], 1800)
	if summary == "" {
		return nil, errors.New("model omitted page summary")
	}
	result := map[string]any{"summary": summary}
	result["page_type"] = choice(
		p["page_type"],
		32,
		"other",
		"cover",
		"table_of_contents",
		"lecture_content",
		"diagram",
		"table",
		"exercise",
		"solution",
		"references",
		"administrative",
		"blank",
		"other",
	)
	result["confidence"] = choice(p["confidence"], 16, "medium", "low", "medium", "high")
	low, _ := p["low_information"].(bool)
	result["low_information"] = low
	for key, limits := range map[string][2]int{
		"key_points":       {10, 300},
		"definitions":      {8, 300},
		"formulas":         {8, 300},
		"visuals":          {8, 350},
		"examples":         {6, 350},
		"assessment_clues": {6, 350},
		"references":       {6, 300},
	} {
		result[key] = textList(p[key], limits[0], limits[1])
	}
	return result, nil
}
