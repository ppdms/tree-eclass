package synthesis

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func (s Service) claim(ctx context.Context, lane string) (job, error) {
	j := job{Lane: lane}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return j, err
	}
	defer tx.Rollback(ctx)
	j.AI, err = settings.ReadAI(ctx, tx)
	if err != nil {
		return j, err
	}
	if !enabled(j.AI, lane) {
		return j, pgx.ErrNoRows
	}
	j.Requested = model(j.AI, lane)
	err = tx.QueryRow(ctx, `SELECT id,course_id FROM `+table(lane)+` WHERE status='pending' AND requested_model=$1 AND analysis_version=$2 AND available_at::timestamptz<=clock_timestamp() ORDER BY priority DESC,available_at::timestamptz,id LIMIT 1`, j.Requested, version(lane)).
		Scan(&j.ID, &j.Course)
	if err != nil {
		return j, err
	}
	if err = loadPacket(ctx, tx, &j); err != nil {
		return j, err
	}
	valid, err := validJob(ctx, tx, j, j.AI)
	if err != nil {
		return j, err
	}
	if !valid {
		return j, abandonStale(ctx, tx, j)
	}
	j.Claim = stamp(time.Now())
	j.Attempts++
	_, err = tx.Exec(
		ctx,
		`UPDATE `+table(lane)+` SET status='running',claimed_at=$2,attempts=attempts+1,error=NULL WHERE id=$1`,
		j.ID,
		j.Claim,
	)
	if err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func loadPacket(ctx context.Context, tx pgx.Tx, j *job) error {
	var locked int64
	if err := tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, j.Course).Scan(&locked); err != nil {
		return err
	}
	fields := `revision_hash,'' unit_key,'' blueprint_revision_hash`
	if j.Lane == "practice" {
		fields = `set_hash,unit_key,blueprint_revision_hash`
	}
	var raw *string
	if err := tx.QueryRow(ctx, `SELECT `+fields+`,attempts,CASE WHEN octet_length(evidence_packet_json)<=2097152 THEN evidence_packet_json END FROM `+table(j.Lane)+` WHERE id=$1 AND status='pending' AND available_at::timestamptz<=clock_timestamp() FOR UPDATE`, j.ID).
		Scan(&j.Hash, &j.Unit, &j.Blueprint, &j.Attempts, &raw); err != nil {
		return err
	}
	if raw == nil {
		return errors.New("synthesis packet exceeds 2 MiB")
	}
	return decode([]byte(*raw), &j.Packet)
}
func abandonStale(ctx context.Context, tx pgx.Tx, j job) error {
	if _, err := tx.Exec(ctx, `UPDATE `+table(j.Lane)+` SET status='stale',finished_at=$2 WHERE id=$1`, j.ID, stamp(time.Now())); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return pgx.ErrNoRows
}
func validJob(ctx context.Context, tx pgx.Tx, j job, a settings.AI) (bool, error) {
	if !enabled(a, j.Lane) || model(a, j.Lane) != j.Requested || a.AnalysisGeneration() != j.AI.AnalysisGeneration() {
		return false, nil
	}
	p, err := settings.ReadExamPlan(ctx, tx, j.Course)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !eligible(p) {
		return false, nil
	}
	expected, err := digest(j.Packet["trusted_planning_context"])
	if err != nil {
		return false, err
	}
	current, err := digest(planning(p))
	if err != nil || current != expected {
		return false, err
	}
	if j.Lane == "practice" {
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.course_blueprints WHERE course_id=$1 AND revision_hash=$2 AND status='ready' AND requested_model=$3 AND analysis_version=$4)`, j.Course, j.Blueprint, a.CourseModel, settings.CourseAnalysisVersion).
			Scan(&exists)
		if err != nil || !exists {
			return false, err
		}
	}
	reason, err := navigation.ValidateEvidence(ctx, tx, j.Course, a, j.Packet)
	return reason == "" && err == nil, err
}
