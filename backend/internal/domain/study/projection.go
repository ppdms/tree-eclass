package study

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/settings"
)

// Increment when the scheduler/intelligence serialization or rules change.
const projectionVersion = "native-study-v1"

func studyDay(now time.Time) time.Time {
	zone, err := time.LoadLocation("Europe/Athens")
	if err != nil {
		panic(err)
	}
	local := now.In(zone)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
}

// A fingerprint of transactionally updated per-course generations avoids a
// global write lock. Readers compare it in the same snapshot as their payload;
// an older publication can never masquerade as current after a committed write.
func fingerprint(ctx context.Context, tx pgx.Tx, now time.Time) (string, settings.AI, error) {
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return "", a, err
	}
	planner, err := settings.ReadPlanner(ctx, tx)
	if err != nil {
		return "", a, err
	}
	var sources []byte
	err = tx.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_array(c.id,g.generation,coalesce(l.generation,0),n.source_generation,n.config_generation,n.content_id) ORDER BY c.id),'[]')
 FROM app.courses c JOIN read_model.course_generation g ON g.course_id=c.id
 LEFT JOIN read_model.learner_generation l ON l.course_id=c.id LEFT JOIN read_model.navigation n ON n.course_id=c.id
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1)`).
		Scan(&sources)
	if err != nil {
		return "", a, err
	}
	raw, err := json.Marshal(struct {
		Version, Day, AI string
		Planner          settings.Planner
		Sources          json.RawMessage
	}{projectionVersion, studyDay(now).Format(time.DateOnly), a.AnalysisGeneration(), planner, sources})
	if err != nil {
		return "", a, err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), a, nil
}

func (s Service) Intelligence(ctx context.Context, selected *int64, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	view, err := intelligenceTx(ctx, tx, selected, now)
	if err != nil {
		return nil, err
	}
	return view, tx.Commit(ctx)
}

func intelligenceTx(ctx context.Context, tx pgx.Tx, selected *int64, now time.Time) (map[string]any, error) {
	var err error
	if _, _, err = courses.SnapshotCourses(ctx, tx, selected); err != nil {
		return nil, err
	}
	fingerprint, _, err := fingerprint(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	scope := "all"
	if selected != nil {
		scope = "course:" + strconv.FormatInt(*selected, 10)
	}
	var raw, stored, status, generated string
	var generation int64
	err = tx.QueryRow(ctx, `SELECT payload_json,source_fingerprint,status,generated_at,generation FROM read_model.study_metrics WHERE scope=$1 AND octet_length(payload_json)<=16777216`, scope).
		Scan(&raw, &stored, &status, &generated, &generation)
	if err == pgx.ErrNoRows || err == nil && stored != fingerprint {
		return pendingIntelligence(), nil
	}
	if err != nil {
		return nil, err
	}
	view := map[string]any{}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&view); err != nil {
		return nil, err
	}
	if view == nil {
		return nil, fmt.Errorf("invalid study projection payload")
	}
	view["generated_at"], view["generation"], view["stale"], view["status"], view["study_projection_status"] = generated, generation, false, status, status
	return view, nil
}

// Full keeps the compatibility response's mutable snapshot and derived-state
// freshness decision in one database snapshot.
func (s Service) Full(ctx context.Context, selected *int64, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	snapshot, err := snapshotTx(ctx, tx, selected, now)
	if err != nil {
		return nil, err
	}
	view, err := intelligenceTx(ctx, tx, selected, now)
	if err != nil {
		return nil, err
	}
	view["courses"] = snapshot.Courses
	view["inbox"] = snapshot.Inbox
	view["planner_rows"] = snapshot.Plans
	view["planner_settings"] = snapshot.Settings
	view["selected_course"] = snapshot.Selected
	return view, tx.Commit(ctx)
}

func pendingIntelligence() map[string]any {
	return map[string]any{
		"study_projection_status":      "pending",
		"adaptive_plan_available":      false,
		"study_intelligence_available": false,
		"stale":                        true,
	}
}
