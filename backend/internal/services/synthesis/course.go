package synthesis

import (
	"context"
	"encoding/json"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

func prepareCourse(ctx context.Context, tx database.Tx, p settings.ExamPlan, a settings.AI) error {
	packet, err := buildCoursePacket(ctx, tx, p, a)
	if err != nil || packet == nil {
		return err
	}
	evidenceHash, err := digest(packet)
	if err != nil {
		return err
	}
	revisionHash, err := digest([]any{p.CourseID, evidenceHash, settings.CourseAnalysisVersion, a.CourseModel})
	if err != nil {
		return err
	}
	raw, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	return tx.Synthesis().QueueCourseRevision(ctx, database.QueueCourseParams{
		CourseID:     p.CourseID,
		RevisionHash: revisionHash,
		EvidenceHash: evidenceHash,
		PacketJSON:   string(raw),
		Version:      settings.CourseAnalysisVersion,
		Model:        a.CourseModel,
		AvailableAt:  stamp(time.Now()),
	})
}

func buildCoursePacket(
	ctx context.Context, tx database.Tx, p settings.ExamPlan, a settings.AI,
) (map[string]any, error) {
	docs, total, ready, err := collectDocuments(ctx, tx, p.CourseID, a)
	if err != nil || len(docs) == 0 {
		return nil, err
	}
	evidence, snapshots, refs := []any{}, []any{}, []any{}
	for _, d := range docs {
		evidence = append(evidence, d.Entry)
		snapshots = append(snapshots, d.Snapshot)
		refs = append(refs, d.Entry["evidence_ref"])
	}
	community, captured, err := collectCommunity(ctx, tx, p.CourseID)
	if err != nil {
		return nil, err
	}
	for _, item := range community {
		evidence = append(evidence, item)
		refs = append(refs, item["evidence_ref"])
	}
	state := "progressive"
	if ready == total && int64(len(docs)) == ready {
		state = "complete"
	}
	return map[string]any{
		"course":                map[string]any{"course_id": p.CourseID, "course_name": p.CourseName},
		"evidence":              evidence,
		"allowed_evidence_refs": refs,
		"source_snapshot": map[string]any{
			"documents":     snapshots,
			"conversations": captured,
		},
		"trusted_planning_context": planning(p),
		"course_progress": map[string]any{
			"coverage_state":               state,
			"documents_total":              total,
			"documents_with_ready_insight": ready,
			"documents_in_packet":          len(docs),
			"documents_omitted":            ready - int64(len(docs)),
		},
	}, nil
}
