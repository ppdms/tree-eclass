package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"tree-eclass/internal/infrastructure/rdbms"
	"tree-eclass/internal/integrations/inference"

	"tree-eclass/internal/domain/settings"
)

var errStale = errors.New("analysis source or settings changed")

func (s Service) readSnapshot(ctx context.Context, j job) (rdbms.Tx, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	d, err := currentDocument(ctx, tx, j.Document.ID, false)
	if err != nil || d.Hash != j.Document.Hash || d.Context != j.Document.Context {
		tx.Rollback(ctx)
		if err != nil {
			return nil, err
		}
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
	var course int64
	err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, j.Document.Course).Scan(&course)
	if errors.Is(err, rdbms.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	d, err := currentDocument(ctx, tx, j.Document.ID, true)
	if err != nil && !errors.Is(err, rdbms.ErrNoRows) {
		return err
	}
	a, settingsErr := settings.ReadAI(ctx, tx)
	if settingsErr != nil {
		return settingsErr
	}
	if err != nil || d.Hash != j.Document.Hash || d.Context != j.Document.Context || a.Model != j.Requested ||
		!a.EnrichmentEnabled {
		if err = finishFailure(ctx, tx, j, "pending", "Source or settings changed; analysis will be reconsidered.", time.Now(), true); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	raw, err := json.Marshal(result.Payload)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if j.Page > 0 {
		err = publishPage(ctx, tx, j, result.Model, string(raw), now)
	} else {
		err = publishDocument(ctx, tx, j, result.Model, string(raw), now)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func publishPage(ctx context.Context, tx rdbms.Tx, j job, model, payload, generatedAt string) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE knowledge.page_enrichments SET status='ready',model=$4,payload_json=$5,generated_at=$6,error=NULL,claimed_at=NULL WHERE document_id=$1 AND page_number=$2 AND claimed_at=$3 AND status='running' AND source_hash=$7 AND requested_model=$8 AND analysis_version=$9`,
		j.Document.ID,
		j.Page,
		j.Claim,
		model,
		payload,
		generatedAt,
		j.Document.Hash,
		j.Requested,
		j.Version,
	)
	return err
}
func publishDocument(ctx context.Context, tx rdbms.Tx, j job, model, payload, generatedAt string) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE knowledge.document_enrichments SET status='ready',model=$3,requested_model=$4,payload_json=$5,generated_at=$6,error=NULL,claimed_at=NULL WHERE document_id=$1 AND claimed_at=$2 AND status='running' AND source_hash=$7 AND context_hash=$8 AND analysis_version=$9`,
		j.Document.ID,
		j.Claim,
		model,
		j.Requested,
		payload,
		generatedAt,
		j.Document.Hash,
		j.Document.Context,
		j.Version,
	)
	return err
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
func finishFailure(ctx context.Context, tx rdbms.Tx, j job, status, message string, at time.Time, reset bool) error {
	if j.Page > 0 {
		_, err := tx.Exec(
			ctx,
			`UPDATE knowledge.page_enrichments SET status=$4,error=$5,available_at=$6,claimed_at=NULL,attempts=CASE WHEN $7 THEN greatest(0,attempts-1) ELSE attempts END WHERE document_id=$1 AND page_number=$2 AND claimed_at=$3 AND status='running'`,
			j.Document.ID,
			j.Page,
			j.Claim,
			status,
			message,
			at.UTC().Format(time.RFC3339Nano),
			reset,
		)
		return err
	}
	_, err := tx.Exec(
		ctx,
		`UPDATE knowledge.document_enrichments SET status=$3,error=$4,available_at=$5,claimed_at=NULL,attempts=CASE WHEN $6 THEN greatest(0,attempts-1) ELSE attempts END WHERE document_id=$1 AND claimed_at=$2 AND status='running'`,
		j.Document.ID,
		j.Claim,
		status,
		message,
		at.UTC().Format(time.RFC3339Nano),
		reset,
	)
	return err
}
