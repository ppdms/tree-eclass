package study

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

func (s Service) Refresh(ctx context.Context, now time.Time) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	fingerprint, a, err := fingerprint(ctx, tx, now)
	if err != nil {
		return false, err
	}
	scope, generation, err := nextScope(ctx, tx, fingerprint)
	if errors.Is(err, database.ErrNoRows) {
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
	published, err := s.Pool.Study().PublishMetric(ctx, database.StudyPublishParams{
		Scope:             scope,
		Payload:           string(raw),
		GeneratedAt:       now,
		SourceFingerprint: fingerprint,
		Status:            status,
		Generation:        generation,
	})
	if err != nil {
		return false, err
	}
	return published, buildErr
}

func nextScope(ctx context.Context, ops database.Operations, fingerprint string) (string, int64, error) {
	scope, err := ops.Study().NextStaleScope(ctx, fingerprint)
	if err != nil {
		return "", 0, err
	}
	return scope.Scope, scope.Generation, nil
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
		raw = append([]byte(nil),
			`{"adaptive_plan_available":false,`+
				`"study_intelligence_available":false,`+
				`"study_projection_error":"The study projection `+
				`could not be prepared. It will be retried."}`...)
	}
	return raw, status, buildErr
}

func (s Service) buildProjection(
	ctx context.Context,
	tx database.Tx,
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
	meta, err := tx.Study().KnowledgeMeta(ctx, selected)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"adaptive_plan":                adaptive,
		"adaptive_plan_available":      true,
		"study_intelligence":           intelligence,
		"study_intelligence_available": true,
		"knowledge_meta": map[string]any{
			"freshness":         meta.Freshness,
			"pending_documents": meta.Pending,
			"failed_documents":  meta.Failed,
		},
	}, nil
}
