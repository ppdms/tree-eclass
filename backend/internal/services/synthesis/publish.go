package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/inference"
)

func (s Service) publish(ctx context.Context, j job, result inference.Generated) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Synthesis().LockCourseForPublish(ctx, j.Course); database.IsNoRows(err) {
		return nil
	} else if err != nil {
		return err
	}
	// Source writers increment this row in their transaction. Holding it through
	// validation/publication serializes the visibility of a concurrent mutation.
	if err = tx.Synthesis().LockGenerationForPublish(ctx, j.Course); err != nil {
		return err
	}
	active, err := tx.Synthesis().ClaimActive(ctx, j.Lane, j.ID, j.Claim)
	if err != nil || !active {
		return err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return err
	}
	valid, err := validJob(ctx, tx, j, a)
	if err != nil {
		return err
	}
	if !valid {
		if err = finish(ctx, tx, j, "stale", "Source or planning settings changed; result discarded.", time.Now(), true); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err = replaceReady(ctx, tx, j, result); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func replaceReady(ctx context.Context, tx database.Tx, j job, result inference.Generated) error {
	now := stamp(time.Now())
	raw, err := json.Marshal(result.Payload)
	if err != nil {
		return err
	}
	if j.Lane == database.SynthesisPractice {
		if err = insertQuestions(ctx, tx, j, raw); err != nil {
			return err
		}
	}
	return tx.Synthesis().PublishReady(ctx, j.Lane, database.PublishParams{
		ID: j.ID, CourseID: j.Course, UnitKey: j.Unit,
		ClaimedAt: j.Claim, Model: result.Model, Payload: string(raw), ReadyAt: now,
	})
}

func insertQuestions(ctx context.Context, tx database.Tx, j job, raw []byte) error {
	var set blueprints.PracticeSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return err
	}
	questions := make([]database.SynthesisQuestion, 0, len(set.Questions))
	for _, q := range set.Questions {
		payload, err := json.Marshal(q)
		if err != nil {
			return err
		}
		questions = append(questions, database.SynthesisQuestion{
			ID: q.ID, Key: q.Key, Mode: q.Mode, Difficulty: q.Difficulty, Minutes: q.Minutes, Payload: string(payload),
		})
	}
	return tx.Synthesis().ReplaceQuestions(ctx, j.Course, j.ID, j.Unit, questions)
}

func (s Service) fail(ctx context.Context, j job, failure error) error {
	status, message, delay, reset := "pending", "Synthesis failed; no partial result was published.", 30*time.Second, false
	var pause inference.Paused
	if errors.As(failure, &pause) {
		delay = max(time.Minute, pause.Delay)
		message = "Providers paused or unavailable; waiting to retry."
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
	if err = finish(ctx, tx, j, status, message, time.Now().Add(delay), reset); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func finish(ctx context.Context, tx database.Tx, j job, status, message string, at time.Time, reset bool) error {
	return tx.Synthesis().FinishClaim(ctx, j.Lane, database.FinishParams{
		ID: j.ID, ClaimedAt: j.Claim, Status: status, Error: message,
		AvailableAt: stamp(at), Reset: reset, FinishedAt: stamp(time.Now()),
	})
}
