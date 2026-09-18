// Package navigation reads published course plans with live learner progress.
// Evidence validation and publication belong to the background processor.
package navigation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type Service struct{ Pool *pgxpool.Pool }
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

const navigationQuery = `WITH actions AS MATERIALIZED (
 SELECT a.action_id,a.unit_key,a.ordinal,a.payload,
 CASE WHEN p.latest_event IN('completed','stuck','deferred') THEN p.latest_event WHEN p.latest_event IS NOT NULL THEN 'in_progress' ELSE 'pending' END status,
 coalesce(p.progress_minutes,0) progress_minutes FROM read_model.roadmap_actions a LEFT JOIN read_model.action_progress p USING(course_id,action_id) WHERE a.course_id=$1
), units AS (SELECT unit_key,count(*) total_actions,count(*) FILTER(WHERE status='completed') completed_actions FROM actions GROUP BY unit_key)
SELECT n.overview,content.payload,coalesce(n.source_generation,-1),g.generation,coalesce(n.config_generation,''),
 coalesce((SELECT jsonb_agg(to_jsonb(u)) FROM units u),'[]'),
 (SELECT to_jsonb(a) FROM actions a WHERE status<>'completed' ORDER BY CASE WHEN status IN('pending','in_progress') THEN 0 ELSE 1 END,ordinal LIMIT 1),
 CASE WHEN $2 AND $3 THEN coalesce((SELECT jsonb_agg(to_jsonb(a) ORDER BY ordinal) FROM actions a WHERE $4::text IS NULL OR a.unit_key=$4),'[]') ELSE '[]'::jsonb END
FROM read_model.course_generation g LEFT JOIN read_model.navigation n USING(course_id)
LEFT JOIN read_model.roadmap_content content ON $2 AND content.content_id=n.content_id WHERE g.course_id=$1`

func (s Service) Read(ctx context.Context, request Request) (View, error) {
	result := View{}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
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
func (s Service) ReadTx(ctx context.Context, tx pgx.Tx, request Request) (View, error) {
	result := View{}
	_, selected, err := courses.SnapshotCourses(ctx, tx, &request.CourseID)
	if err != nil {
		return result, err
	}
	if selected.Hidden && !request.IncludeHidden {
		return result, pgx.ErrNoRows
	}
	result.Course = *selected
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	var overview, content, units, next, actions []byte
	var built, current int64
	var config string
	err = tx.QueryRow(ctx, navigationQuery, request.CourseID, request.Roadmap, request.IncludeActions, request.Unit).
		Scan(&overview, &content, &built, &current, &config, &units, &next, &actions)
	if err != nil {
		return result, err
	}
	if len(overview) == 0 || built != current || config != a.AnalysisGeneration() {
		result.Blueprint = pending(request.CourseID)
		return result, nil
	}
	raw := overview
	if request.Roadmap {
		raw = content
	}
	result.SourceBytes = len(raw) + len(units) + len(next) + len(actions)
	if err = decodeJSON(raw, &result.Blueprint); err != nil {
		return result, err
	}
	if result.Blueprint == nil {
		return result, errors.New("stored navigation payload is not an object")
	}
	result.Blueprint = identity.DecodeJSON(result.Blueprint).(map[string]any)
	if err = decorateProgress(result.Blueprint, units, next, actions, request); err != nil {
		return result, err
	}
	return result, nil
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
