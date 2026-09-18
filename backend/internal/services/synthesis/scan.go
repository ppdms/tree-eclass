package synthesis

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/settings"
)

func (s Service) prepare(ctx context.Context, lane string) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil || !enabled(a, lane) {
		return err
	}
	var course int64
	err = tx.QueryRow(ctx, `SELECT c.id FROM app.courses c JOIN app.course_exam_plans p ON p.course_id=c.id
 JOIN read_model.course_generation g ON g.course_id=c.id LEFT JOIN read_model.synthesis_scan s ON s.course_id=c.id AND s.lane=$1
 WHERE p.enabled=1 AND p.exam_at IS NOT NULL AND p.exam_at<>'' AND p.commitment<>'skipped'
 AND (s.course_id IS NULL OR s.source_generation<>g.generation OR s.config_generation<>$2 OR s.next_at<=now())
 ORDER BY s.next_at NULLS FIRST,c.id LIMIT 1 FOR UPDATE OF c SKIP LOCKED`, lane, a.AnalysisGeneration()).Scan(&course)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	plan, err := settings.ReadExamPlan(ctx, tx, course)
	if err != nil {
		return err
	}
	if lane == "course" {
		err = prepareCourse(ctx, tx, plan, a)
	} else {
		err = preparePractice(ctx, tx, plan, a)
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO read_model.synthesis_scan(course_id,lane,source_generation,config_generation,next_at)
 SELECT course_id,$2,generation,$3,$4 FROM read_model.course_generation WHERE course_id=$1
 ON CONFLICT(course_id,lane) DO UPDATE SET source_generation=excluded.source_generation,config_generation=excluded.config_generation,next_at=excluded.next_at`,
		course,
		lane,
		a.AnalysisGeneration(),
		time.Now().Add(5*time.Minute),
	)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
