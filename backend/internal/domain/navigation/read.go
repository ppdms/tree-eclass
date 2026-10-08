// Package navigation reads published course plans with live learner progress.
// Evidence validation and publication belong to the background processor.
package navigation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type Service struct{ Pool database.Store }
type Request struct {
	CourseID                               int64
	Roadmap, IncludeHidden, IncludeActions bool
	Unit                                   *string
}
type View struct {
	SourceBytes int            `json:"-"`
	Course      courses.Course `json:"course"`
	Blueprint   map[string]any `json:"course_blueprint"`
}

func (s Service) Read(ctx context.Context, request Request) (View, error) {
	result := View{}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = s.ReadTx(ctx, tx, request)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

// ReadTx shares an existing repeatable-read snapshot with study/practice views.
func (s Service) ReadTx(ctx context.Context, tx database.Tx, request Request) (View, error) {
	result := View{}
	selected, err := loadReadCourse(ctx, tx, request)
	if err != nil {
		return result, err
	}
	result.Course = *selected
	raw, err := loadReadPayload(ctx, tx, request)
	if err != nil {
		return result, err
	}
	if raw == nil {
		result.Blueprint = pending(request.CourseID)
		return result, nil
	}
	return result, finishRead(ctx, tx, request, &result, raw)
}

// loadReadCourse resolves the visible course row for the request.
func loadReadCourse(ctx context.Context, ops database.Operations, request Request) (*courses.Course, error) {
	_, selected, err := courses.SnapshotCourses(ctx, ops, &request.CourseID)
	if err != nil {
		return nil, err
	}
	if selected.Hidden && !request.IncludeHidden {
		return nil, database.ErrNoRows
	}
	return selected, nil
}

// loadReadPayload reads the plan payload and checks it against the current
// generation stamps. A nil payload with a nil error means the cached plan is
// stale and the caller should serve the pending blueprint.
func loadReadPayload(ctx context.Context, ops database.Operations, request Request) ([]byte, error) {
	a, err := settings.ReadAI(ctx, ops)
	if err != nil {
		return nil, err
	}
	payload, err := ops.Navigation().Payload(ctx, request.CourseID, request.Roadmap)
	if err != nil {
		return nil, err
	}
	if len(payload.Overview) == 0 || payload.Built != payload.Current || payload.Config != a.AnalysisGeneration() {
		return nil, nil
	}
	if request.Roadmap {
		return payload.Content, nil
	}
	return payload.Overview, nil
}

// finishRead attaches live progress to the plan payload and decodes it into
// the view blueprint.
func finishRead(ctx context.Context, ops database.Operations, request Request, result *View, raw []byte) error {
	units, err := marshalUnits(ctx, ops, request.CourseID)
	if err != nil {
		return err
	}
	next, err := marshalNext(ctx, ops, request.CourseID)
	if err != nil {
		return err
	}
	actions := []byte("[]")
	if request.Roadmap && request.IncludeActions {
		var unit *string
		if request.Unit != nil {
			encoded := identity.Encode(*request.Unit)
			unit = &encoded
		}
		actions, err = marshalActions(ctx, ops, request.CourseID, unit)
		if err != nil {
			return err
		}
	}
	result.SourceBytes = len(raw) + len(units) + len(next) + len(actions)
	if err = decodeJSON(raw, &result.Blueprint); err != nil {
		return err
	}
	if result.Blueprint == nil {
		return errors.New("stored navigation payload is not an object")
	}
	result.Blueprint = identity.DecodeJSON(result.Blueprint).(map[string]any)
	return decorateProgress(result.Blueprint, units, next, actions, request)
}

// marshalUnits assembles the per-unit progress stats decorateProgress reads
// from explicit rows. An empty course yields [].
func marshalUnits(ctx context.Context, ops database.Operations, course int64) ([]byte, error) {
	rows, err := ops.Navigation().UnitProgress(ctx, course)
	if err != nil {
		return nil, err
	}
	stats := []unitStats{}
	for _, row := range rows {
		stats = append(stats, unitStats{Key: row.Key, Total: row.Total, Complete: row.Complete})
	}
	return json.Marshal(stats)
}

// marshalAction assembles one action blob in the shape decorateProgress
// decodes. The stored payload is JSON text already, so it embeds verbatim
// (exact number formatting preserved); key order is irrelevant because the
// consumer decodes into actionRow.
func marshalAction(payload []byte, status string, minutes int64) ([]byte, error) {
	return json.Marshal(struct {
		Payload json.RawMessage `json:"payload"`
		Status  string          `json:"status"`
		Minutes int64           `json:"progress_minutes"`
	}{Payload: json.RawMessage(payload), Status: status, Minutes: minutes})
}

// marshalNext assembles the single next-action blob, or nil when every action
// is completed. decorateProgress treats an empty blob as no next action, the
// way it treated the old NULL subselect.
func marshalNext(ctx context.Context, ops database.Operations, course int64) ([]byte, error) {
	row, err := ops.Navigation().NextAction(ctx, course)
	if errors.Is(err, database.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return marshalAction(row.Payload, row.Status, row.Minutes)
}

// marshalActions assembles the ordered action blobs for the roadmap view,
// optionally restricted to one unit. A nil unit selects every unit.
func marshalActions(ctx context.Context, ops database.Operations, course int64, unit *string) ([]byte, error) {
	rows, err := ops.Navigation().ListActions(ctx, course, unit)
	if err != nil {
		return nil, err
	}
	list := []json.RawMessage{}
	for _, row := range rows {
		raw, err := marshalAction(row.Payload, row.Status, row.Minutes)
		if err != nil {
			return nil, err
		}
		list = append(list, raw)
	}
	return json.Marshal(list)
}

func pending(course int64) map[string]any {
	return map[string]any{
		"course_id": course,
		"usable":    false,
		"readiness": map[string]any{"state": "pending", "reason": "The course projection is being prepared."},
		"progress":  map[string]any{"completed_actions": 0, "total_actions": 0, "percent": 0, "next_action": nil},
	}
}

func decodeJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}
