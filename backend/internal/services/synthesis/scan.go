package synthesis

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/settings"
)

func (s Service) prepare(ctx context.Context, lane database.SynthesisLane) error {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil || !enabled(a, lane) {
		return err
	}
	// Expensive scans coordinate with Analysis/Indexing admission so only one
	// heavy pipeline pass runs at a time.
	if err = tx.Synthesis().AcquireExpensive(ctx); err != nil {
		return err
	}
	selected, err := tx.Synthesis().ScanDueCourse(ctx, lane, a.AnalysisGeneration())
	if database.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	plan, err := settings.ReadExamPlan(ctx, tx, selected.CourseID)
	if err != nil {
		return err
	}
	if lane == database.SynthesisCourse {
		err = prepareCourse(ctx, tx, plan, a)
	} else {
		err = preparePractice(ctx, tx, plan, a)
	}
	if err != nil {
		return err
	}
	if err = tx.Synthesis().RecordScan(ctx, lane, selected.CourseID, a.AnalysisGeneration(),
		stamp(time.Now().Add(5*time.Minute))); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
