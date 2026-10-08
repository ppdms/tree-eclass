package practice

import (
	"context"
	"encoding/json"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type currentQuestion struct{ Unit, Key, Set, Revision string }

func admitQuestion(ctx context.Context, tx database.Tx, course int64, id string) (currentQuestion, error) {
	result := currentQuestion{}
	if err := tx.Practice().LockCourse(ctx, course); err != nil {
		return result, err
	}
	generation, err := tx.Practice().LockedGeneration(ctx, course)
	if err != nil {
		return result, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	admitted, err := tx.Practice().AdmissionQuestion(ctx, database.PracticeAdmissionSelector{
		CourseID:        course,
		Generation:      generation,
		Config:          a.AnalysisGeneration(),
		AnalysisVersion: settings.PracticeAnalysisVersion,
		Model:           a.PracticeModel,
		Question:        id,
	})
	if database.IsNoRows(err) {
		return result, ErrConflict
	}
	if err != nil {
		return result, err
	}
	result.Unit, result.Key, result.Set, result.Revision =
		admitted.Unit, admitted.Key, admitted.Set, admitted.Revision
	return result, validateAdmission(course, id, result, admitted.Payload, admitted.Evidence)
}

func validateAdmission(course int64, id string, result currentQuestion, payload *string, evidence []byte) error {
	if payload == nil {
		return ErrConflict
	}
	var q blueprints.Question
	if err := json.Unmarshal([]byte(*payload), &q); err != nil {
		return ErrConflict
	}
	var packet struct {
		EvidenceLinks map[string]any `json:"evidence_links"`
		Blueprint     struct {
			Units []struct {
				Key string `json:"key"`
			} `json:"units"`
		} `json:"blueprint"`
	}
	if err := json.Unmarshal(evidence, &packet); err != nil {
		return ErrConflict
	}
	// The legacy EXISTS(jsonb_array_elements(payload#>'{blueprint,units}'))
	// membership test has no portable SQL form, so the unit membership check
	// happens here in Go.
	member := false
	for _, unit := range packet.Blueprint.Units {
		if unit.Key == result.Unit {
			member = true
			break
		}
	}
	if !member {
		return ErrConflict
	}
	links := identity.DecodeJSON(packet.EvidenceLinks).(map[string]any)
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
