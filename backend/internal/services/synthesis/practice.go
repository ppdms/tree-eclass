package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func preparePractice(ctx context.Context, tx database.Tx, p settings.ExamPlan, a settings.AI) error {
	blueprint, err := tx.Synthesis().ReadyBlueprint(ctx, p.CourseID, a.CourseModel, settings.CourseAnalysisVersion)
	if database.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if blueprint.Payload == nil || blueprint.Packet == nil {
		return errors.New("blueprint exceeds synthesis input budget")
	}
	var packet map[string]any
	if err = decode([]byte(*blueprint.Packet), &packet); err != nil {
		return err
	}
	validated, err := blueprints.Validate([]byte(*blueprint.Payload), packet)
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
	for _, unit := range validated.Units[:min(40, len(validated.Units))] {
		input, err := practicePacket(packet, validated, unit)
		if err != nil {
			return err
		}
		if err = queuePractice(ctx, tx, p.CourseID, blueprint.Revision, unit.Key, input, a); err != nil {
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
	tx database.Tx,
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
	return tx.Synthesis().QueuePracticeRevision(ctx, database.QueuePracticeParams{
		CourseID:     course,
		UnitKey:      unit,
		Revision:     revision,
		SetHash:      hash,
		EvidenceHash: evidenceHash,
		PacketJSON:   string(raw),
		Version:      settings.PracticeAnalysisVersion,
		Model:        a.PracticeModel,
		AvailableAt:  stamp(time.Now()),
	})
}
