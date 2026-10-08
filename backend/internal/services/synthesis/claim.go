package synthesis

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func (s Service) claim(ctx context.Context, lane database.SynthesisLane) (job, error) {
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
		return j, database.ErrNoRows
	}
	j.Requested = model(j.AI, lane)
	selected, err := tx.Synthesis().ClaimDueRow(ctx, lane, j.Requested, version(lane))
	if err != nil {
		return j, err
	}
	j.ID, j.Course = selected.ID, selected.CourseID
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
	if err = tx.Synthesis().MarkClaimed(ctx, lane, j.ID, j.Claim); err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}

func loadPacket(ctx context.Context, tx database.Tx, j *job) error {
	packet, err := tx.Synthesis().LoadPacket(ctx, j.Lane, j.ID)
	if err != nil {
		return err
	}
	j.Hash, j.Unit, j.Blueprint, j.Attempts = packet.Hash, packet.UnitKey, packet.Blueprint, packet.Attempts
	return decode([]byte(packet.PacketJSON), &j.Packet)
}

func abandonStale(ctx context.Context, tx database.Tx, j job) error {
	if err := tx.Synthesis().AbandonClaim(ctx, j.Lane, j.ID, stamp(time.Now())); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return database.ErrNoRows
}

func validJob(ctx context.Context, tx database.Tx, j job, a settings.AI) (bool, error) {
	if !enabled(a, j.Lane) || model(a, j.Lane) != j.Requested || a.AnalysisGeneration() != j.AI.AnalysisGeneration() {
		return false, nil
	}
	p, err := settings.ReadExamPlan(ctx, tx, j.Course)
	if database.IsNoRows(err) {
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
	if j.Lane == database.SynthesisPractice {
		exists, err := tx.Synthesis().BlueprintReady(
			ctx, j.Course, j.Blueprint, a.CourseModel, settings.CourseAnalysisVersion,
		)
		if err != nil || !exists {
			return false, err
		}
	}
	reason, err := navigation.ValidateEvidence(ctx, tx, j.Course, a, j.Packet)
	return reason == "" && err == nil, err
}
