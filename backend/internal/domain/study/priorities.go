package study

import (
	"fmt"
	"math"
	"sort"
	"time"

	"tree-eclass/internal/domain/settings"
)

type PriorityMaterial struct {
	ordinal     int64
	ID          string         `json:"id"`
	CourseID    int64          `json:"course_id"`
	CourseName  string         `json:"course_name"`
	Name        string         `json:"display_name"`
	Path        string         `json:"source_path"`
	Origin      string         `json:"source_origin"`
	Kind        string         `json:"document_kind"`
	Complexity  int64          `json:"complexity_score"`
	Reading     *int64         `json:"reading_minutes"`
	Pages       *int64         `json:"page_count"`
	Words       *int64         `json:"word_count"`
	Enriched    bool           `json:"enriched"`
	AI          map[string]any `json:"ai"`
	Level       int64          `json:"level"`
	Priority    float64        `json:"priority"`
	Days        *int64         `json:"days_left"`
	Recommended string         `json:"recommended_action"`
}

type materialRunway struct {
	Plan             settings.ExamPlan
	Date             time.Time
	Total, Completed float64
	Count, Essential int64
	Materials        int64
	Next             []*PriorityMaterial
}
type priorities struct {
	sequence        int64
	Today           time.Time
	Selected        *int64
	Total, Enriched int64
	Runways         map[int64]*materialRunway
	Top             []*PriorityMaterial
	ByCourse        map[int64][]*PriorityMaterial
}

func newPriorities(plans []settings.ExamPlan, selected *int64, today time.Time) *priorities {
	p := &priorities{
		Today:    today,
		Selected: selected,
		Runways:  map[int64]*materialRunway{},
		Top:      []*PriorityMaterial{},
		ByCourse: map[int64][]*PriorityMaterial{},
	}
	for _, plan := range plans {
		if !plan.Enabled || plan.ExamAt == nil {
			continue
		}
		exam, err := examDate(*plan.ExamAt)
		if err != nil {
			continue
		}
		p.Runways[plan.CourseID] = &materialRunway{Plan: plan, Date: exam, Next: []*PriorityMaterial{}}
	}
	return p
}

func examDate(raw string) (time.Time, error) {
	if len(raw) > 10 {
		raw = raw[:10]
	}
	return time.Parse(time.DateOnly, raw)
}
func category(value any, weights map[string]float64, fallback float64) float64 {
	key, _ := value.(string)
	if v, ok := weights[key]; ok {
		return v
	}
	return fallback
}

var importanceWeights = map[string]float64{"essential": 2.6, "useful": 1.6, "reference": .65}
var assessmentWeights = map[string]float64{"high": 1.55, "medium": 1.2, "low": .75, "unknown": .95}
var difficultyWeights = map[string]float64{"advanced": 1.25, "intermediate": 1.08, "introductory": .95}

func materialPriority(m *PriorityMaterial) float64 {
	if m.AI["material_type"] == "results" || m.AI["material_type"] == "administrative" {
		return 0
	}
	gap, ok := map[int64]float64{0: 1, 1: .82, 2: .6, 3: .34, 4: 0, 5: 0}[m.Level]
	if !ok {
		gap = 1
	}
	density := 1 + min(.25, float64(m.Complexity)/400)
	return gap * category(
		m.AI["importance"],
		importanceWeights,
		1.15,
	) * category(
		m.AI["assessment_relevance"],
		assessmentWeights,
		.95,
	) * category(
		m.AI["difficulty"],
		difficultyWeights,
		1,
	) * density
}

// Add retains only the candidates needed for the final response, even when a
// course has thousands of documents. Aggregate coverage still includes them all.
func (p *priorities) Add(m *PriorityMaterial) {
	p.sequence++
	m.ordinal = p.sequence
	scoped := p.Selected == nil || *p.Selected == m.CourseID
	if scoped {
		p.Total++
		if m.Enriched {
			p.Enriched++
		}
	}
	m.Priority = math.RoundToEven(materialPriority(m)*1000) / 1000
	m.Recommended, _ = m.AI["recommended_action"].(string)
	if m.Recommended == "" {
		m.Recommended = "Review this material"
	}
	if r := p.Runways[m.CourseID]; r != nil {
		days := max(0, (r.Date.Unix()-p.Today.Unix())/86400)
		m.Days = &days
		r.Materials++
		if m.Level != 5 && m.AI["material_type"] != "results" && m.AI["material_type"] != "administrative" {
			weight := category(m.AI["importance"], importanceWeights, 1)
			r.Total += weight
			r.Completed += weight * float64(min(4, m.Level)) / 4
			if m.Priority > 0 {
				r.Count++
				if m.AI["importance"] == "essential" {
					r.Essential++
				}
				r.Next = topMaterials(r.Next, m, 3, false)
			}
		}
	}
	if !scoped || m.Priority <= 0 {
		return
	}
	p.Top = topMaterials(p.Top, m, 8, true)
	p.ByCourse[m.CourseID] = topMaterials(p.ByCourse[m.CourseID], m, 2, true)
}

func lessMaterial(a, b *PriorityMaterial, course bool) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if course && a.CourseName != b.CourseName {
		return a.CourseName < b.CourseName
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	return a.ordinal < b.ordinal
}

func topMaterials(rows []*PriorityMaterial, m *PriorityMaterial, limit int, course bool) []*PriorityMaterial {
	rows = append(rows, m)
	sort.SliceStable(rows, func(i, j int) bool { return lessMaterial(rows[i], rows[j], course) })
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func (p *priorities) result() map[string]any {
	diverse := []*PriorityMaterial{}
	for _, rows := range p.ByCourse {
		diverse = append(diverse, rows...)
	}
	sort.SliceStable(diverse, func(i, j int) bool { return lessMaterial(diverse[i], diverse[j], true) })
	queue := diverse[:min(8, len(diverse))]
	seen := map[string]bool{}
	for _, m := range queue {
		seen[m.ID] = true
	}
	for _, m := range p.Top {
		if len(queue) >= 8 {
			break
		}
		if !seen[m.ID] {
			queue = append(queue, m)
			seen[m.ID] = true
		}
	}
	runways, all := p.runwayRows()
	orderRunways(all)
	orderRunways(runways)
	percent := int64(0)
	if p.Total > 0 {
		percent = int64(math.RoundToEven(100 * float64(p.Enriched) / float64(p.Total)))
	}
	if p.Enriched < p.Total {
		percent = min(99, percent)
	}
	return map[string]any{
		"focus_queue":     queue,
		"exam_runways":    runways,
		"exam_collisions": collisions(all, p.Selected),
		"coverage":        map[string]int64{"enriched": p.Enriched, "total": p.Total, "percent": percent},
	}
}

func (p *priorities) runwayRows() ([]map[string]any, []map[string]any) {
	runways, all := []map[string]any{}, []map[string]any{}
	for id, r := range p.Runways {
		if r.Materials == 0 || r.Date.Before(p.Today) {
			continue
		}
		name := r.Plan.CourseName
		if r.Plan.ShortName != nil && *r.Plan.ShortName != "" {
			name = *r.Plan.ShortName
		}
		readiness := int64(0)
		if r.Total > 0 {
			readiness = int64(math.RoundToEven(100 * r.Completed / r.Total))
		}
		row := map[string]any{
			"course_id":           id,
			"course_name":         name,
			"exam_at":             r.Plan.ExamAt,
			"exam_date":           r.Date.Format(time.DateOnly),
			"days_left":           (r.Date.Unix() - p.Today.Unix()) / 86400,
			"readiness":           readiness,
			"remaining_count":     r.Count,
			"essential_remaining": r.Essential,
			"next_materials":      r.Next,
		}
		all = append(all, row)
		if p.Selected == nil || *p.Selected == id {
			runways = append(runways, row)
		}
	}
	return runways, all
}

func orderRunways(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a["exam_date"] != b["exam_date"] {
			return a["exam_date"].(string) < b["exam_date"].(string)
		}
		return a["course_id"].(int64) < b["course_id"].(int64)
	})
}

func collisions(rows []map[string]any, selected *int64) []map[string]any {
	result := []map[string]any{}
	for i := 1; i < len(rows); i++ {
		first, second := rows[i-1], rows[i]
		if selected != nil && first["course_id"] != *selected && second["course_id"] != *selected {
			continue
		}
		a, _ := examDate(first["exam_date"].(string))
		b, _ := examDate(second["exam_date"].(string))
		gap := (b.Unix() - a.Unix()) / 86400
		if gap > 4 {
			continue
		}
		label := fmt.Sprintf("%d days apart", gap)
		if gap == 0 {
			label = "the same day"
		}
		if gap == 1 {
			label = "1 day apart"
		}
		result = append(
			result,
			map[string]any{
				"first":    first,
				"second":   second,
				"gap_days": gap,
				"message":  "These exams are " + label + ", so their final review windows overlap.",
			},
		)
	}
	return result
}
