package practice

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type currentQuestion struct{ Unit, Key, Set, Revision string }

func admitQuestion(ctx context.Context, tx pgx.Tx, course int64, id string) (currentQuestion, error) {
	result := currentQuestion{}
	var locked, generation int64
	if err := tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=app.courses.id AND p.enabled=1)) FOR SHARE`, course).Scan(&locked); err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=$1 FOR SHARE`, course).Scan(&generation); err != nil {
		return result, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	var payload *string
	var evidence []byte
	err = tx.QueryRow(ctx, `SELECT q.unit_key,q.question_key,s.set_hash,s.blueprint_revision_hash,
 CASE WHEN octet_length(q.payload_json)<=65536 THEN q.payload_json END,c.payload->'evidence_links'
 FROM read_model.navigation n JOIN read_model.roadmap_content c ON c.content_id=n.content_id
 JOIN knowledge.practice_question_sets s ON s.course_id=n.course_id AND s.blueprint_revision_hash=n.revision_id
 JOIN knowledge.practice_questions q ON q.set_id=s.id AND q.course_id=s.course_id AND q.unit_key=s.unit_key
 WHERE n.course_id=$1 AND n.source_generation=$2 AND n.config_generation=$3 AND n.overview->>'usable'='true'
 AND s.status='ready' AND s.analysis_version=$4 AND s.requested_model=$5 AND q.question_id=$6
 AND EXISTS(SELECT 1 FROM jsonb_array_elements(c.payload#>'{blueprint,units}') u WHERE u->>'key'=s.unit_key)
 FOR SHARE OF n,s,q`,
		course,
		generation,
		a.AnalysisGeneration(),
		settings.PracticeAnalysisVersion,
		a.PracticeModel,
		id,
	).Scan(
		&result.Unit,
		&result.Key,
		&result.Set,
		&result.Revision,
		&payload,
		&evidence,
	)
	if err == pgx.ErrNoRows {
		return result, ErrConflict
	}
	if err != nil {
		return result, err
	}
	return result, validateAdmission(course, id, result, payload, evidence)
}

func validateAdmission(course int64, id string, result currentQuestion, payload *string, evidence []byte) error {
	if payload == nil {
		return ErrConflict
	}
	var q blueprints.Question
	if err := json.Unmarshal([]byte(*payload), &q); err != nil {
		return ErrConflict
	}
	var links map[string]any
	if err := json.Unmarshal(evidence, &links); err != nil {
		return ErrConflict
	}
	links = identity.DecodeJSON(links).(map[string]any)
	known := map[string]bool{}
	for ref := range links {
		known[ref] = true
	}
	if err := q.PracticeQuestion.Validate(known); err != nil {
		return ErrConflict
	}
	if q.ID != id || q.Key != result.Key || id != blueprints.QuestionIdentity(course, result.Unit, q.Prompt, q.Mode) {
		return ErrConflict
	}
	return nil
}
