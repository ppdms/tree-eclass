package study

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

func (s Service) Refresh(ctx context.Context, now time.Time) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	fingerprint, a, err := fingerprint(ctx, tx, now)
	if err != nil {
		return false, err
	}
	scope, generation, err := nextScope(ctx, tx, fingerprint)
	if errors.Is(err, rdbms.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	selected, err := scopeSelector(scope)
	if err != nil {
		return false, err
	}
	view, buildErr := s.buildProjection(ctx, tx, selected, a, studyDay(now))
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	raw, status, buildErr := projectionPayload(view, buildErr)
	// No source mutation occurs in this transaction. Close its snapshot before
	// publishing, including when a failed database read aborted the snapshot.
	if err = tx.Rollback(ctx); err != nil {
		return false, err
	}
	published, err := publishMetric(ctx, s.Pool, scope, string(raw), now, fingerprint, status, generation)
	if err != nil {
		return false, err
	}
	return published, buildErr
}

// publishMetric writes one projection row. Postgres uses a single
// conditional upsert; sqlite has no ON CONFLICT WHERE, so both drivers share
// this portable update-then-insert: the UPDATE carries the generation guard,
// the INSERT fills the absent row. Single-writer sqlite and the caller's
// advisory posture make the two statements atomic enough in practice.
func publishMetric(
	ctx context.Context,
	db rdbms.DBTX,
	scope, payload string,
	now time.Time,
	fingerprint, status string,
	generation int64,
) (bool, error) {
	result, err := db.Exec(
		ctx,
		`UPDATE read_model.study_metrics SET payload_json=$2,generated_at=$3,generation=generation+1,
 source_fingerprint=$4,status=$5,retry_after=CASE WHEN $5='failed' THEN now()+interval '30 seconds' END
 WHERE scope=$1 AND generation=$6`,
		scope,
		payload,
		now.UTC().Format(time.RFC3339Nano),
		fingerprint,
		status,
		generation,
	)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	result, err = db.Exec(
		ctx,
		`INSERT INTO read_model.study_metrics(scope,payload_json,generated_at,generation,source_fingerprint,status,retry_after)
 VALUES($1,$2,$3,1,$4,$5,CASE WHEN $5='failed' THEN now()+interval '30 seconds' END)
 ON CONFLICT(scope) DO NOTHING`,
		scope,
		payload,
		now.UTC().Format(time.RFC3339Nano),
		fingerprint,
		status,
	)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func nextScope(ctx context.Context, tx rdbms.Tx, fingerprint string) (string, int64, error) {
	var scope string
	var generation int64
	err := tx.QueryRow(ctx, `WITH scopes AS (
 SELECT 'all' scope UNION ALL SELECT 'course:'||c.id FROM app.courses c
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1)
) SELECT s.scope,coalesce(m.generation,0) FROM scopes s LEFT JOIN read_model.study_metrics m USING(scope)
 WHERE m.scope IS NULL OR m.source_fingerprint<>$1 OR (m.status='failed' AND m.retry_after<=now())
 ORDER BY m.generated_at NULLS FIRST,s.scope LIMIT 1`, fingerprint).Scan(&scope, &generation)
	return scope, generation, err
}

func scopeSelector(scope string) (*int64, error) {
	if !strings.HasPrefix(scope, "course:") {
		return nil, nil
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(scope, "course:"), 10, 64)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func projectionPayload(view map[string]any, buildErr error) ([]byte, string, error) {
	raw, err := json.Marshal(view)
	if err != nil {
		buildErr = errors.Join(buildErr, err)
	}
	if len(raw) > 16*1024*1024 {
		buildErr = errors.Join(buildErr, errors.New("study projection exceeds 16 MiB"))
	}
	status := "ready"
	if buildErr != nil {
		status = "failed"
		raw = []byte(
			`{"adaptive_plan_available":false,"study_intelligence_available":false,"study_projection_error":"The study projection could not be prepared. It will be retried."}`,
		)
	}
	return raw, status, buildErr
}

func (s Service) buildProjection(
	ctx context.Context,
	tx rdbms.Tx,
	selected *int64,
	a settings.AI,
	today time.Time,
) (map[string]any, error) {
	adaptive, err := s.buildAdaptive(ctx, tx, selected, today)
	if err != nil {
		return nil, err
	}
	intelligence, err := buildIntelligence(ctx, tx, selected, a, today)
	if err != nil {
		return nil, err
	}
	var pending, failed int64
	var freshness *string
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE d.status IN('pending','running')),count(*) FILTER(WHERE d.status='failed'),max(d.indexed_at)
 FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id WHERE d.is_current=1
 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1)) AND ($1::bigint IS NULL OR d.course_id=$1)`, selected).
		Scan(&pending, &failed, &freshness)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"adaptive_plan":                adaptive,
		"adaptive_plan_available":      true,
		"study_intelligence":           intelligence,
		"study_intelligence_available": true,
		"knowledge_meta": map[string]any{
			"freshness":         freshness,
			"pending_documents": pending,
			"failed_documents":  failed,
		},
	}, nil
}
