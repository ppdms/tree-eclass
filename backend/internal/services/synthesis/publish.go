package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"tree-eclass/internal/infrastructure/rdbms"

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
	var locked int64
	err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, j.Course).Scan(&locked)
	if errors.Is(err, rdbms.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	// Source writers increment this row in their transaction. Holding it through
	// validation/publication serializes the visibility of a concurrent mutation.
	if err = tx.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=$1 FOR UPDATE`, j.Course).Scan(&locked); err != nil {
		return err
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+table(j.Lane)+` WHERE id=$1 AND claimed_at=$2 AND status='running')`, j.ID, j.Claim).Scan(&active); err != nil ||
		!active {
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
func replaceReady(ctx context.Context, tx rdbms.Tx, j job, result inference.Generated) error {
	now := stamp(time.Now())
	scope := `course_id=$1`
	args := []any{j.Course, now}
	if j.Lane == "practice" {
		scope += ` AND unit_key=$3`
		args = append(args, j.Unit)
	}
	if _, err := tx.Exec(ctx, `UPDATE `+table(j.Lane)+` SET status='stale',finished_at=$2 WHERE `+scope+` AND status='ready'`, args...); err != nil {
		return err
	}
	raw, err := json.Marshal(result.Payload)
	if err != nil {
		return err
	}
	if j.Lane == "practice" {
		if err = insertQuestions(ctx, tx, j, raw); err != nil {
			return err
		}
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE `+table(
			j.Lane,
		)+` SET status='ready',model=$3,payload_json=$4,generated_at=$5,finished_at=$5,claimed_at=NULL,error=NULL WHERE id=$1 AND claimed_at=$2 AND status='running'`,
		j.ID,
		j.Claim,
		result.Model,
		string(raw),
		now,
	)
	return err
}
func insertQuestions(ctx context.Context, tx rdbms.Tx, j job, raw []byte) error {
	var set blueprints.PracticeSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM knowledge.practice_questions WHERE set_id=$1`, j.ID); err != nil {
		return err
	}
	for i, q := range set.Questions {
		payload, err := json.Marshal(q)
		if err != nil {
			return err
		}
		_, err = tx.Exec(
			ctx,
			`INSERT INTO knowledge.practice_questions(question_id,set_id,course_id,unit_key,ordinal,question_key,response_mode,difficulty,estimated_minutes,payload_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			q.ID,
			j.ID,
			j.Course,
			j.Unit,
			i+1,
			q.Key,
			q.Mode,
			q.Difficulty,
			q.Minutes,
			string(payload),
		)
		if err != nil {
			return err
		}
	}
	return nil
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
func finish(ctx context.Context, tx rdbms.Tx, j job, status, message string, at time.Time, reset bool) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE `+table(
			j.Lane,
		)+` SET status=$3,error=$4,available_at=$5,claimed_at=NULL,attempts=CASE WHEN $6 THEN greatest(0,attempts-1) ELSE attempts END,finished_at=CASE WHEN $3 IN('stale','failed') THEN $7 ELSE NULL END WHERE id=$1 AND claimed_at=$2 AND status='running'`,
		j.ID,
		j.Claim,
		status,
		message,
		stamp(at),
		reset,
		stamp(time.Now()),
	)
	return err
}
