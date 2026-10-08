package courses

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
)

type Coverage struct {
	Course
	Completion   float64          `json:"completion_ratio"`
	Total        int64            `json:"total_files"`
	Indexed      int64            `json:"indexed_files"`
	Distribution map[string]int64 `json:"study_distribution"`
	Stale        bool             `json:"stale"`
}
type RecentMaterial struct {
	ID         string  `json:"id"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Path       string  `json:"source_path"`
	Name       string  `json:"display_name"`
	Indexed    *string `json:"indexed_at"`
}
type Shelf struct {
	Courses    []Coverage                  `json:"courses"`
	Levels     map[string]map[string]int64 `json:"study_levels"`
	Recent     []RecentMaterial            `json:"recent_materials"`
	Generated  *string                     `json:"generated_at"`
	Generation int64                       `json:"generation"`
	Status     string                      `json:"status"`
	Stale      bool                        `json:"stale"`
}
type coveragePayload struct {
	Completion   float64          `json:"completion_ratio"`
	Total        int64            `json:"total_files"`
	Indexed      int64            `json:"indexed_files"`
	Distribution map[string]int64 `json:"study_distribution"`
	Levels       map[string]int64 `json:"study_levels"`
}

// Shelf never rebuilds source facts. A committed source or learner mutation
// immediately marks just that course stale until the processor catches up.
func (s Service) Shelf(ctx context.Context) (Shelf, error) {
	result := Shelf{
		Courses: []Coverage{},
		Levels:  map[string]map[string]int64{},
		Recent:  []RecentMaterial{},
		Status:  "ready",
	}
	rows, err := s.Pool.Courses().ShelfRows(ctx)
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		if err = appendShelfRow(row, &result); err != nil {
			return result, err
		}
	}
	if result.Stale {
		result.Status = "pending"
	}
	result.Recent = sortShelfRecent(result.Recent)
	return result, nil
}

// appendShelfRow merges one shelf row's course, levels, and materials into
// the result, tracking staleness and the oldest stamp.
func appendShelfRow(row database.ShelfRow, result *Shelf) error {
	item, levels, materials, err := decodeCoverage(row.Course, row.Payload, row.Recent)
	if err != nil {
		return err
	}
	item.Stale = row.Stale
	result.Courses = append(result.Courses, item)
	result.Levels[strconv.FormatInt(item.ID, 10)] = levels
	if !row.Stale {
		result.Recent = append(result.Recent, materials...)
	}
	result.Stale = result.Stale || row.Stale
	result.Generation = max(result.Generation, row.Generation)
	if row.Generated != nil && (result.Generated == nil || *row.Generated < *result.Generated) {
		result.Generated = row.Generated
	}
	return nil
}

// sortShelfRecent orders recent materials newest-first and caps the shelf at
// the six latest entries.
func sortShelfRecent(recent []RecentMaterial) []RecentMaterial {
	slices.SortFunc(recent, func(a, b RecentMaterial) int {
		left, right := "", ""
		if a.Indexed != nil {
			left = *a.Indexed
		}
		if b.Indexed != nil {
			right = *b.Indexed
		}
		if cmp := strings.Compare(right, left); cmp != 0 {
			return cmp
		}
		return strings.Compare(b.ID, a.ID)
	})
	return recent[:min(6, len(recent))]
}

func decodeCoverage(
	row database.AppCourse, payload *string, recent []byte,
) (Coverage, map[string]int64, []RecentMaterial, error) {
	item := Coverage{}
	p := coveragePayload{
		Distribution: emptyDistribution(), Levels: map[string]int64{},
	}
	materials := []RecentMaterial{}
	item.Course = course(row)
	if payload != nil {
		if err := json.Unmarshal([]byte(*payload), &p); err != nil {
			return item, nil, nil, err
		}
	}
	if len(recent) > 0 {
		if err := json.Unmarshal(recent, &materials); err != nil {
			return item, nil, nil, err
		}
	}
	item.Completion, item.Total, item.Indexed = p.Completion, p.Total, p.Indexed
	item.Distribution = p.Distribution
	return item, p.Levels, materials, nil
}

func emptyDistribution() map[string]int64 {
	return map[string]int64{"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
}
