// Package navigation reads published course plans with live learner progress.
// Evidence validation and publication belong to the background processor.
package navigation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Service struct{ Pool rdbms.Pool }
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

// navigationBaseQuery loads the immutable plan payloads. Progress aggregates
// use explicit columns assembled in Go below so the queries stay portable:
// row-form JSON constructors have no sqlite form and fail at prepare time.
const navigationBaseQuery = `SELECT n.overview,content.payload,coalesce(n.source_generation,-1),g.generation,coalesce(n.config_generation,'')
FROM read_model.course_generation g LEFT JOIN read_model.navigation n USING(course_id)
LEFT JOIN read_model.roadmap_content content ON $2 AND content.content_id=n.content_id WHERE g.course_id=$1`

// navigationActionsCTE shares the action/progress join across the progress
// queries. Status derives from the latest learner event; the unit counts use
// count(CASE...) so both drivers aggregate identically.
const navigationActionsCTE = `WITH actions AS (
 SELECT a.action_id,a.unit_key,a.ordinal,a.payload,
 CASE WHEN p.latest_event IN('completed','stuck','deferred') THEN p.latest_event WHEN p.latest_event IS NOT NULL THEN 'in_progress' ELSE 'pending' END status,
 coalesce(p.progress_minutes,0) progress_minutes FROM read_model.roadmap_actions a LEFT JOIN read_model.action_progress p USING(course_id,action_id) WHERE a.course_id=$1
)`

const navigationUnitsQuery = navigationActionsCTE + ` SELECT unit_key,count(*) total_actions,count(CASE WHEN status='completed' THEN 1 END) completed_actions FROM actions GROUP BY unit_key`

const navigationNextQuery = navigationActionsCTE + ` SELECT payload,status,progress_minutes FROM actions WHERE status<>'completed' ORDER BY CASE WHEN status IN('pending','in_progress') THEN 0 ELSE 1 END,ordinal LIMIT 1`

const navigationActionsQuery = navigationActionsCTE + ` SELECT payload,status,progress_minutes FROM actions WHERE ($2 IS NULL OR unit_key=$2) ORDER BY ordinal`

func (s Service) Read(ctx context.Context, request Request) (View, error) {
	result := View{}
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
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
func (s Service) ReadTx(ctx context.Context, tx rdbms.Tx, request Request) (View, error) {
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
func loadReadCourse(ctx context.Context, tx rdbms.Tx, request Request) (*courses.Course, error) {
	_, selected, err := courses.SnapshotCourses(ctx, tx, &request.CourseID)
	if err != nil {
		return nil, err
	}
	if selected.Hidden && !request.IncludeHidden {
		return nil, rdbms.ErrNoRows
	}
	return selected, nil
}

// loadReadPayload reads the plan payload and checks it against the current
// generation stamps. A nil payload with a nil error means the cached plan is
// stale and the caller should serve the pending blueprint.
func loadReadPayload(ctx context.Context, tx rdbms.Tx, request Request) ([]byte, error) {
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	var overview, content []byte
	var built, current int64
	var config string
	err = tx.QueryRow(ctx, navigationBaseQuery, request.CourseID, request.Roadmap).
		Scan(&overview, &content, &built, &current, &config)
	if err != nil {
		return nil, err
	}
	if len(overview) == 0 || built != current || config != a.AnalysisGeneration() {
		return nil, nil
	}
	if request.Roadmap {
		return content, nil
	}
	return overview, nil
}

// finishRead attaches live progress to the plan payload and decodes it into
// the view blueprint.
func finishRead(ctx context.Context, tx rdbms.Tx, request Request, result *View, raw []byte) error {
	units, err := marshalUnits(ctx, tx, request.CourseID)
	if err != nil {
		return err
	}
	next, err := marshalNext(ctx, tx, request.CourseID)
	if err != nil {
		return err
	}
	actions := []byte("[]")
	if request.Roadmap && request.IncludeActions {
		var unit any
		if request.Unit != nil {
			unit = identity.Encode(*request.Unit)
		}
		actions, err = marshalActions(ctx, tx, request.CourseID, unit)
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
// from explicit columns. Counts stay numbers; an empty course yields [] like
// the old coalesce(jsonb_agg(...),'[]').
func marshalUnits(ctx context.Context, tx rdbms.Tx, course int64) ([]byte, error) {
	rows, err := tx.Query(ctx, navigationUnitsQuery, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stats := []unitStats{}
	for rows.Next() {
		var stat unitStats
		if err = rows.Scan(&stat.Key, &stat.Total, &stat.Complete); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	if err = rows.Err(); err != nil {
		return nil, err
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
func marshalNext(ctx context.Context, tx rdbms.Tx, course int64) ([]byte, error) {
	var payload []byte
	var status string
	var minutes int64
	err := tx.QueryRow(ctx, navigationNextQuery, course).Scan(&payload, &status, &minutes)
	if errors.Is(err, rdbms.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return marshalAction(payload, status, minutes)
}

// marshalActions assembles the ordered action blobs for the roadmap view,
// optionally restricted to one unit. A nil unit selects every unit, matching
// the old ($2 IS NULL OR unit_key=$2) filter.
func marshalActions(ctx context.Context, tx rdbms.Tx, course int64, unit any) ([]byte, error) {
	rows, err := tx.Query(ctx, navigationActionsQuery, course, unit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []json.RawMessage{}
	for rows.Next() {
		var payload []byte
		var status string
		var minutes int64
		if err = rows.Scan(&payload, &status, &minutes); err != nil {
			return nil, err
		}
		raw, err := marshalAction(payload, status, minutes)
		if err != nil {
			return nil, err
		}
		list = append(list, raw)
	}
	if err = rows.Err(); err != nil {
		return nil, err
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
