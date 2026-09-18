// Package blueprints validates generated plans before they can enter navigation
// or influence learner scheduling. Model output cannot invent evidence IDs.
package blueprints

type Blueprint struct {
	Version   string     `json:"blueprint_version"`
	Strategy  Strategy   `json:"exam_strategy"`
	Units     []Unit     `json:"units"`
	Families  []Family   `json:"question_families"`
	Conflicts []Conflict `json:"conflicts"`
	Gaps      []Gap      `json:"coverage_gaps"`
}
type Strategy struct {
	Objective  string   `json:"objective"`
	Approach   string   `json:"approach"`
	Target     float64  `json:"target_score_percent"`
	Confidence string   `json:"confidence"`
	Priorities []string `json:"priority_unit_keys"`
	Evidence   []string `json:"evidence_refs"`
}
type Unit struct {
	Key       string   `json:"key"`
	Title     string   `json:"title"`
	Objective string   `json:"objective"`
	Priority  string   `json:"priority"`
	Estimated int64    `json:"estimated_minutes"`
	Depends   []string `json:"depends_on"`
	Evidence  []string `json:"evidence_refs"`
	Actions   []Action `json:"actions"`
}
type Action struct {
	Kind        string   `json:"kind"`
	Instruction string   `json:"instruction"`
	Estimated   int64    `json:"estimated_minutes"`
	Success     string   `json:"success_criterion"`
	Evidence    []string `json:"evidence_refs"`
}
type Family struct {
	Key        string   `json:"key"`
	Name       string   `json:"name"`
	Mode       string   `json:"response_mode"`
	Priority   string   `json:"priority"`
	Confidence string   `json:"confidence"`
	Observed   int64    `json:"observed_count"`
	OutOf      int64    `json:"observed_out_of"`
	Marks      float64  `json:"estimated_marks_percent"`
	Units      []string `json:"unit_keys"`
	Evidence   []string `json:"evidence_refs"`
}
type Conflict struct {
	Summary    string   `json:"summary"`
	Status     string   `json:"status"`
	Resolution string   `json:"resolution"`
	Evidence   []string `json:"evidence_refs"`
}
type Gap struct {
	Summary  string   `json:"summary"`
	Severity string   `json:"severity"`
	Action   string   `json:"recommended_action"`
	Evidence []string `json:"evidence_refs"`
}
