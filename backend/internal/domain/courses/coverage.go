package courses

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/storage/queries"
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
	rows, err := s.Pool.Query(ctx, `SELECT to_jsonb(c),p.payload_json,p.recent_json,p.generated_at,
 coalesce(p.generation<>g.generation OR p.learner_generation<>coalesce(l.generation,0),true),coalesce(p.generation,0)
 FROM app.courses c JOIN read_model.course_generation g ON g.course_id=c.id
 LEFT JOIN read_model.learner_generation l ON l.course_id=c.id
 LEFT JOIN read_model.course_coverage p ON p.course_id=c.id
 WHERE c.hidden=0 ORDER BY c.sort_order,c.id`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw, recent []byte
		var payload *string
		var stamp *string
		var generation int64
		var stale bool
		if err = rows.Scan(&raw, &payload, &recent, &stamp, &stale, &generation); err != nil {
			return result, err
		}
		item, levels, materials, err := decodeCoverage(raw, payload, recent)
		if err != nil {
			return result, err
		}
		item.Stale = stale
		result.Courses = append(result.Courses, item)
		result.Levels[strconv.FormatInt(item.ID, 10)] = levels
		if !stale {
			result.Recent = append(result.Recent, materials...)
		}
		result.Stale = result.Stale || stale
		result.Generation = max(result.Generation, generation)
		if stamp != nil && (result.Generated == nil || *stamp < *result.Generated) {
			result.Generated = stamp
		}
	}
	if result.Stale {
		result.Status = "pending"
	}
	slices.SortFunc(result.Recent, func(a, b RecentMaterial) int {
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
	result.Recent = result.Recent[:min(6, len(result.Recent))]
	return result, rows.Err()
}

func decodeCoverage(raw []byte, payload *string, recent []byte) (Coverage, map[string]int64, []RecentMaterial, error) {
	var row queries.AppCourse
	item := Coverage{}
	p := coveragePayload{Distribution: emptyDistribution(), Levels: map[string]int64{}}
	materials := []RecentMaterial{}
	if err := json.Unmarshal(raw, &row); err != nil {
		return item, nil, nil, err
	}
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
	item.Completion, item.Total, item.Indexed, item.Distribution = p.Completion, p.Total, p.Indexed, p.Distribution
	return item, p.Levels, materials, nil
}

func emptyDistribution() map[string]int64 {
	return map[string]int64{"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
}

func visibleCourse(ctx context.Context, tx pgx.Tx, id int64) error {
	var found int64
	return tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`, id).Scan(&found)
}
