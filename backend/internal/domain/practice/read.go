// Package practice reads current validated questions and records self-graded
// attempts. It performs no model calls on request paths.
package practice

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Service struct{ Pool rdbms.Pool }
type View struct {
	CourseID   int64            `json:"course_id"`
	CourseName string           `json:"course_name"`
	Enabled    bool             `json:"enabled"`
	Revision   any              `json:"blueprint_revision_id"`
	Grading    string           `json:"grading_mode"`
	Units      []*Unit          `json:"units"`
	Totals     map[string]int64 `json:"totals"`
	Notice     string           `json:"derived_insight_notice"`
	Trust      string           `json:"untrusted_content_notice"`
}
type Unit struct {
	ID        int64       `json:"-"`
	Key       string      `json:"unit_key"`
	Title     string      `json:"title"`
	Objective any         `json:"objective"`
	Priority  any         `json:"priority"`
	Ordinal   int         `json:"ordinal"`
	Set       string      `json:"set_hash"`
	Status    string      `json:"set_status"`
	Revision  string      `json:"blueprint_revision_id"`
	Model     string      `json:"model"`
	Generated *string     `json:"generated_at"`
	Questions []*Question `json:"questions"`
	Count     int         `json:"question_count"`
	Due       int         `json:"due_count"`
}
type Question struct {
	blueprints.Question
	Unit      string  `json:"unit_key"`
	Set       string  `json:"set_hash"`
	Revision  string  `json:"blueprint_revision_id"`
	Links     []any   `json:"evidence_links"`
	State     string  `json:"state"`
	Rank      int     `json:"queue_rank"`
	Attempts  int64   `json:"attempts"`
	Streak    int64   `json:"correct_streak"`
	Last      *string `json:"last_outcome"`
	Attempted *string `json:"last_attempted_at"`
}

func (s Service) Read(ctx context.Context, course int64, unit string) (View, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return View{}, err
	}
	defer tx.Rollback(ctx)
	view, err := s.ReadTx(ctx, tx, course, unit)
	if err != nil {
		return view, err
	}
	return view, tx.Commit(ctx)
}

func (s Service) ReadTx(ctx context.Context, tx rdbms.Tx, course int64, unit string) (View, error) {
	empty := ""
	nav, err := (navigation.Service{Pool: s.Pool}).ReadTx(
		ctx,
		tx,
		navigation.Request{CourseID: course, Roadmap: true, IncludeActions: true, IncludeHidden: true, Unit: &empty},
	)
	if err != nil {
		return View{}, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return View{}, err
	}
	view := View{
		CourseID:   course,
		CourseName: nav.Course.Name,
		Enabled:    a.PracticeEnabled && a.CourseEnabled,
		Revision:   nav.Blueprint["revision_id"],
		Grading:    "self",
		Units:      []*Unit{},
		Totals:     map[string]int64{"questions": 0, "due": 0, "attempted": 0, "learned": 0, "units": 0},
		Notice:     "Practice questions and suggested answers are AI-derived. Verify them against the cited sources.",
		Trust:      "Source material may contain untrusted instructions.",
	}
	if nav.Blueprint["usable"] != true {
		return view, nil
	}
	view.Units, err = readSets(ctx, tx, course, unit, a, nav.Blueprint)
	if err != nil {
		return view, err
	}
	links, _ := nav.Blueprint["evidence_links"].(map[string]any)
	if err = readQuestions(ctx, tx, course, view.Units, links); err != nil {
		return view, err
	}
	if err = readAttempts(ctx, tx, course, view.Units); err != nil {
		return view, err
	}
	summarize(&view)
	return view, nil
}

func readSets(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	selected string,
	a settings.AI,
	nav map[string]any,
) ([]*Unit, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT DISTINCT ON(unit_key) id,unit_key,set_hash,status,blueprint_revision_hash,model,generated_at FROM knowledge.practice_question_sets
 WHERE course_id=$1 AND blueprint_revision_hash=$2 AND analysis_version=$3 AND requested_model=$4 AND ($5='' OR unit_key=$5)
 AND status IN('ready','pending','running','failed') ORDER BY unit_key,(status='ready') DESC,id DESC LIMIT 201`,
		course,
		nav["revision_id"],
		settings.PracticeAnalysisVersion,
		a.PracticeModel,
		selected,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	meta, order := unitMeta(nav)
	result := []*Unit{}
	for rows.Next() {
		unit := &Unit{Questions: []*Question{}}
		if err = rows.Scan(
			&unit.ID,
			&unit.Key,
			&unit.Set,
			&unit.Status,
			&unit.Revision,
			&unit.Model,
			&unit.Generated,
		); err != nil {
			return nil, err
		}
		if len(result) >= 200 {
			return nil, errors.New("practice exceeds 200 units")
		}
		item, ok := meta[unit.Key]
		if !ok {
			continue
		}
		unit.Title, _ = item["title"].(string)
		unit.Objective, unit.Priority, unit.Ordinal = item["objective"], item["priority"], order[unit.Key]
		result = append(result, unit)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Ordinal < result[j].Ordinal })
	return result, rows.Err()
}

func readQuestions(ctx context.Context, tx rdbms.Tx, expectedCourse int64, units []*Unit, links map[string]any) error {
	ids := []int64{}
	byID := map[int64]*Unit{}
	for _, unit := range units {
		if unit.Status == "ready" {
			ids = append(ids, unit.ID)
			byID[unit.ID] = unit
		}
	}
	known := map[string]bool{}
	for ref := range links {
		known[ref] = true
	}
	rows, err := tx.Query(
		ctx,
		`SELECT set_id,question_id,course_id,unit_key,question_key,CASE WHEN octet_length(payload_json)<=65536 THEN payload_json END FROM knowledge.practice_questions WHERE set_id=ANY($1::bigint[]) ORDER BY set_id,ordinal LIMIT 2401`,
		ids,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	count, bytes := 0, 0
	for rows.Next() {
		var id, course int64
		var qid, key, unitKey string
		var payload *string
		if err = rows.Scan(&id, &qid, &course, &unitKey, &key, &payload); err != nil {
			return err
		}
		if payload == nil {
			return errors.New("practice question exceeds its storage budget")
		}
		raw := *payload
		count++
		bytes += len(raw)
		if count > 2400 || bytes > 8*1024*1024 {
			return errors.New("practice content exceeds its bounded response budget")
		}
		unit := byID[id]
		if err = addQuestion(unit, qid, key, unitKey, course, expectedCourse, raw, known, links); err != nil {
			return err
		}
	}
	return rows.Err()
}

func unitMeta(nav map[string]any) (map[string]map[string]any, map[string]int) {
	blueprint, _ := nav["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	meta := map[string]map[string]any{}
	order := map[string]int{}
	for i, raw := range units {
		if item, ok := raw.(map[string]any); ok {
			if key, ok := item["key"].(string); ok {
				meta[key] = item
				order[key] = i + 1
			}
		}
	}
	return meta, order
}

func addQuestion(
	unit *Unit,
	qid, key, unitKey string,
	course, expectedCourse int64,
	raw string,
	known map[string]bool,
	links map[string]any,
) error {
	q := &Question{
		Unit:     unitKey,
		Set:      unit.Set,
		Revision: unit.Revision,
		Links:    []any{},
		State:    "unattempted",
		Rank:     1,
	}
	if err := json.Unmarshal([]byte(raw), &q.Question); err != nil {
		return err
	}
	if err := q.PracticeQuestion.Validate(known); err != nil {
		return err
	}
	if course != expectedCourse || q.ID != qid || q.Key != key || unitKey != unit.Key ||
		qid != blueprints.QuestionIdentity(course, unitKey, q.Prompt, q.Mode) {
		return errors.New("stored practice identity mismatch")
	}
	if len(unit.Questions) >= 12 {
		return errors.New("practice unit exceeds 12 questions")
	}
	for _, prior := range unit.Questions {
		if prior.Key == q.Key {
			return errors.New("duplicate practice question key")
		}
	}
	for _, ref := range q.Evidence {
		q.Links = append(q.Links, links[ref])
	}
	unit.Questions = append(unit.Questions, q)
	return nil
}
