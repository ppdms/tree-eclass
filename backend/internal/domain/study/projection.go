package study

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
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
func fingerprint(ctx context.Context, ops database.Operations, now time.Time) (string, settings.AI, error) {
	a, err := settings.ReadAI(ctx, ops)
	if err != nil {
		return "", a, err
	}
	planner, err := settings.ReadPlanner(ctx, ops)
	if err != nil {
		return "", a, err
	}
	sources, err := marshalSources(ctx, ops)
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

// marshalSources assembles the per-course generation rows the fingerprint
// hashes. Element order mirrors the old build_array argument order, so
// fingerprints stay stable.
func marshalSources(ctx context.Context, ops database.Operations) ([]byte, error) {
	rows, err := ops.Study().ListSourceRows(ctx)
	if err != nil {
		return nil, err
	}
	sources := []json.RawMessage{}
	for _, row := range rows {
		elem, err := json.Marshal([]any{
			row.Course, row.CourseGen, row.Learner, nullableInt(row.Source), nullableText(row.Config), nullableText(row.Content),
		})
		if err != nil {
			return nil, err
		}
		sources = append(sources, elem)
	}
	return json.Marshal(sources)
}

// nullableInt renders a NULL generation as JSON null, else the number.
func nullableInt(raw *int64) any {
	if raw == nil {
		return nil
	}
	return *raw
}

// nullableText renders a NULL text column as JSON null, else the string.
func nullableText(raw *string) any {
	if raw == nil {
		return nil
	}
	return *raw
}

func (s Service) Intelligence(ctx context.Context, selected *int64, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
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

func intelligenceTx(ctx context.Context, tx database.Tx, selected *int64, now time.Time) (map[string]any, error) {
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
	metric, err := tx.Study().StudyMetric(ctx, scope)
	if errors.Is(err, database.ErrNoRows) || err == nil && metric.SourceFingerprint != fingerprint {
		return pendingIntelligence(), nil
	}
	if err != nil {
		return nil, err
	}
	view := map[string]any{}
	decoder := json.NewDecoder(strings.NewReader(metric.Payload))
	decoder.UseNumber()
	if err = decoder.Decode(&view); err != nil {
		return nil, err
	}
	if view == nil {
		return nil, fmt.Errorf("invalid study projection payload")
	}
	stamp := map[string]any{"generated_at": metric.GeneratedAt, "generation": metric.Generation,
		"stale": false, "status": metric.Status, "study_projection_status": metric.Status}
	for key, value := range stamp {
		view[key] = value
	}
	return view, nil
}

// Full keeps the compatibility response's mutable snapshot and derived-state
// freshness decision in one database snapshot.
func (s Service) Full(ctx context.Context, selected *int64, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
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
