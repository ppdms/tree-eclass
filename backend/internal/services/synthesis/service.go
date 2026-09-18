// Package synthesis produces validated course blueprints and practice sets from
// bounded, source-bound packets. No request handler performs this work.
package synthesis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/inference"
)

type Service struct {
	Pool      *pgxpool.Pool
	Generator inference.Generator
	Keys      map[string]string
}
type job struct {
	ID, Course, Attempts                          int64
	Lane, Unit, Hash, Blueprint, Requested, Claim string
	Packet                                        map[string]any
	AI                                            settings.AI
}

func table(lane string) string {
	if lane == "practice" {
		return "knowledge.practice_question_sets"
	}
	return "knowledge.course_blueprints"
}
func model(a settings.AI, lane string) string {
	if lane == "practice" {
		return a.PracticeModel
	}
	return a.CourseModel
}
func version(lane string) string {
	if lane == "practice" {
		return settings.PracticeAnalysisVersion
	}
	return settings.CourseAnalysisVersion
}
func enabled(a settings.AI, lane string) bool {
	return a.EnrichmentEnabled && a.CourseEnabled && (lane != "practice" || a.PracticeEnabled)
}
func planning(p settings.ExamPlan) map[string]any {
	return map[string]any{
		"exam_at":        p.ExamAt,
		"commitment":     p.Commitment,
		"target_grade":   p.TargetGrade,
		"planning_notes": p.Notes,
	}
}
func eligible(p settings.ExamPlan) bool {
	return p.Enabled && p.ExamAt != nil && *p.ExamAt != "" && p.Commitment != "skipped"
}
func decode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(target)
}
func asMap(value any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = decode(raw, &result)
	return result, err
}
func digest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return blueprints.PayloadHash(raw)
}
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func (s Service) Recover(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, lane := range []string{"course", "practice"} {
		if _, err = tx.Exec(ctx, `UPDATE `+table(lane)+` SET status='pending',claimed_at=NULL,attempts=greatest(0,attempts-1),available_at=$1 WHERE status='running'`, stamp(time.Now())); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RunOne fairly scans one course and then consumes at most one durable claim.
func (s Service) RunOne(ctx context.Context, lane string) (bool, error) {
	if lane != "course" && lane != "practice" {
		return false, errors.New("invalid synthesis lane")
	}
	if err := s.prepare(ctx, lane); err != nil {
		return false, err
	}
	j, err := s.claim(ctx, lane)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	jobctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	result, err := s.generate(jobctx, j)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if err != nil {
		return true, s.fail(ctx, j, err)
	}
	return true, s.publish(ctx, j, result)
}
