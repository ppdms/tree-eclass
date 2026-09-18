package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/settings"
)

func prepareCourse(ctx context.Context, tx pgx.Tx, p settings.ExamPlan, a settings.AI) error {
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
	now := stamp(time.Now())
	if _, err = tx.Exec(ctx, `UPDATE knowledge.course_blueprints SET status='stale',finished_at=$2,claimed_at=NULL WHERE course_id=$1 AND revision_hash<>$3 AND status IN('pending','running')`, p.CourseID, now, revisionHash); err != nil {
		return err
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM knowledge.course_blueprints WHERE revision_hash=$1`, revisionHash).
		Scan(&status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = tx.Exec(
			ctx,
			`UPDATE knowledge.course_blueprints SET status='pending',revision=(SELECT max(revision)+1 FROM knowledge.course_blueprints WHERE course_id=$1),attempts=0,claimed_at=NULL,error=NULL,available_at=$3,created_at=$3,finished_at=NULL,payload_json=NULL,generated_at=NULL WHERE revision_hash=$2`,
			p.CourseID,
			revisionHash,
			now,
		)
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO knowledge.course_blueprints(course_id,revision,revision_hash,evidence_hash,evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)
 SELECT $1,coalesce(max(revision),0)+1,$2,$3,$4,$5,$6,$6,$7,$7 FROM knowledge.course_blueprints WHERE course_id=$1`,
		p.CourseID,
		revisionHash,
		evidenceHash,
		string(raw),
		settings.CourseAnalysisVersion,
		a.CourseModel,
		now,
	)
	return err
}
func buildCoursePacket(ctx context.Context, tx pgx.Tx, p settings.ExamPlan, a settings.AI) (map[string]any, error) {
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
