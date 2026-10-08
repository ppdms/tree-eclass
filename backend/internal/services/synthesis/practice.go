package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func preparePractice(ctx context.Context, tx rdbms.Tx, p settings.ExamPlan, a settings.AI) error {
	var revision string
	var payload, raw *string
	err := tx.QueryRow(ctx, `SELECT revision_hash,CASE WHEN octet_length(payload_json)<=1048576 THEN payload_json END,CASE WHEN octet_length(evidence_packet_json)<=2097152 THEN evidence_packet_json END
 FROM knowledge.course_blueprints WHERE course_id=$1 AND status='ready' AND requested_model=$2 AND analysis_version=$3`, p.CourseID, a.CourseModel, settings.CourseAnalysisVersion).
		Scan(&revision, &payload, &raw)
	if errors.Is(err, rdbms.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if payload == nil || raw == nil {
		return errors.New("blueprint exceeds synthesis input budget")
	}
	var packet map[string]any
	if err = decode([]byte(*raw), &packet); err != nil {
		return err
	}
	blueprint, err := blueprints.Validate([]byte(*payload), packet)
	if err != nil {
		return err
	}
	reason, err := navigation.ValidateEvidence(ctx, tx, p.CourseID, a, packet)
	if err != nil || reason != "" {
		return err
	}
	// Only a blueprint matching the current planning context can seed questions.
	before, err := digest(packet["trusted_planning_context"])
	if err != nil {
		return err
	}
	current, err := digest(planning(p))
	if err != nil || before != current {
		return err
	}
	for _, unit := range blueprint.Units[:min(40, len(blueprint.Units))] {
		input, err := practicePacket(packet, blueprint, unit)
		if err != nil {
			return err
		}
		if err = queuePractice(ctx, tx, p.CourseID, revision, unit.Key, input, a); err != nil {
			return err
		}
	}
	return nil
}
func practicePacket(packet map[string]any, b blueprints.Blueprint, u blueprints.Unit) (map[string]any, error) {
	known := map[string]bool{}
	for _, ref := range u.Evidence {
		known[ref] = true
	}
	for _, action := range u.Actions {
		for _, ref := range action.Evidence {
			known[ref] = true
		}
	}
	families := []blueprints.Family{}
	for _, f := range b.Families {
		if slices.Contains(f.Units, u.Key) {
			families = append(families, f)
			for _, ref := range f.Evidence {
				known[ref] = true
			}
		}
	}
	items, refs := []any{}, []any{}
	for _, raw := range packet["evidence"].([]any) {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("invalid saved evidence")
		}
		ref, _ := item["evidence_ref"].(string)
		if known[ref] {
			items = append(items, item)
			refs = append(refs, ref)
		}
	}
	unit, err := asMap(u)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"course":                   packet["course"],
		"unit":                     unit,
		"question_families":        families,
		"evidence":                 items,
		"allowed_evidence_refs":    refs,
		"source_snapshot":          packet["source_snapshot"],
		"trusted_planning_context": packet["trusted_planning_context"],
	}, nil
}

func queuePractice(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	revision, unit string,
	packet map[string]any,
	a settings.AI,
) error {
	evidenceHash, err := digest(packet)
	if err != nil {
		return err
	}
	hash, err := digest([]any{course, unit, revision, evidenceHash, settings.PracticeAnalysisVersion, a.PracticeModel})
	if err != nil {
		return err
	}
	raw, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	now := stamp(time.Now())
	if _, err = tx.Exec(ctx, `UPDATE knowledge.practice_question_sets SET status='stale',claimed_at=NULL,finished_at=$3 WHERE course_id=$1 AND unit_key=$2 AND set_hash<>$4 AND status IN('pending','running')`, course, unit, now, hash); err != nil {
		return err
	}
	return filePracticeRevision(ctx, tx, course, unit, revision, hash, evidenceHash, string(raw), now, a)
}
func filePracticeRevision(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	unit, revision, hash, evidenceHash, payload, now string,
	a settings.AI,
) error {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM knowledge.practice_question_sets WHERE set_hash=$1`, hash).Scan(&status)
	if err != nil && !errors.Is(err, rdbms.ErrNoRows) {
		return err
	}
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = tx.Exec(
			ctx,
			`UPDATE knowledge.practice_question_sets SET status='pending',attempts=0,claimed_at=NULL,error=NULL,available_at=$2,finished_at=NULL,payload_json=NULL,generated_at=NULL WHERE set_hash=$1`,
			hash,
			now,
		)
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO knowledge.practice_question_sets(course_id,unit_key,set_hash,blueprint_revision_hash,evidence_hash,evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$9)`,
		course,
		unit,
		hash,
		revision,
		evidenceHash,
		payload,
		settings.PracticeAnalysisVersion,
		a.PracticeModel,
		now,
	)
	return err
}
