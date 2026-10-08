package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/inference"
)

var errStale = errors.New("analysis source or settings changed")

func (s Service) readSnapshot(ctx context.Context, j job) (database.Tx, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	current, err := tx.Analysis().CurrentDocument(ctx, j.Document.ID, false)
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	d := fromAnalysisDocument(current)
	if d.Hash != j.Document.Hash || d.Context != j.Document.Context {
		tx.Rollback(ctx)
		return nil, errStale
	}
	return tx, nil
}
func (s Service) publish(ctx context.Context, j job, result inference.Generated) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Analysis().LockCourseForClaim(ctx, j.Document.Course); err != nil {
		if errors.Is(err, database.ErrNoRows) {
			return nil
		}
		return err
	}
	current, err := tx.Analysis().CurrentDocument(ctx, j.Document.ID, true)
	if err != nil && !errors.Is(err, database.ErrNoRows) {
		return err
	}
	a, settingsErr := settings.ReadAI(ctx, tx)
	if settingsErr != nil {
		return settingsErr
	}
	d := document{}
	if err == nil {
		d = fromAnalysisDocument(current)
	}
	if err != nil || d.Hash != j.Document.Hash || d.Context != j.Document.Context || a.Model != j.Requested ||
		!a.EnrichmentEnabled {
		if err = finishFailure(ctx, tx, j, "pending",
			"Source or settings changed; analysis will be reconsidered.", time.Now(), true); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err := publishReadyResult(ctx, tx, j, result); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func publishReadyResult(ctx context.Context, tx database.Tx, j job, result inference.Generated) error {
	raw, err := json.Marshal(result.Payload)
	if err != nil {
		return err
	}
	return tx.Analysis().PublishReady(ctx, database.AnalysisPublishParams{
		DocumentID: j.Document.ID, Page: j.Page, ClaimedAt: j.Claim,
		Model: result.Model, Requested: j.Requested, Payload: string(raw),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Hash:        j.Document.Hash, ContextHash: j.Document.Context, Version: j.Version,
	})
}
func (s Service) fail(ctx context.Context, j job, failure error) error {
	status, message, delay, reset := "pending", "Analysis failed; no partial result was published.", 30*time.Second, false
	var pause inference.Paused
	if errors.As(failure, &pause) {
		delay = max(time.Minute, pause.Delay)
		message = "Providers are paused or unavailable; waiting to retry."
		reset = true
	} else if j.Attempts >= 5 {
		status = "failed"
	} else {
		delays := []time.Duration{30 * time.Second, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour}
		delay = delays[min(max(0, int(j.Attempts)-1), 3)]
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = finishFailure(ctx, tx, j, status, message, time.Now().Add(delay), reset); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func finishFailure(ctx context.Context, tx database.Tx, j job, status, message string, at time.Time, reset bool) error {
	return tx.Analysis().FinishClaim(ctx, database.AnalysisFinishParams{
		DocumentID:  j.Document.ID,
		Page:        j.Page,
		ClaimedAt:   j.Claim,
		Status:      status,
		Error:       message,
		AvailableAt: at.UTC().Format(time.RFC3339Nano),
		Reset:       reset,
	})
}
