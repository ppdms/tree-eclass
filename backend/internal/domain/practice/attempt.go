package practice

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

var ErrInvalid = errors.New("invalid practice attempt")
var ErrConflict = errors.New("practice request conflicts with saved attempt or current question")
var attemptKey = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)
var scores = map[string]float64{"correct": 1, "partial": 0.5, "incorrect": 0, "skipped": 0}

type Attempt struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	Unit       string  `json:"unit_key"`
	Question   string  `json:"question_id"`
	Outcome    string  `json:"outcome"`
	Key        string  `json:"idempotency_key"`
	Confidence *int64  `json:"confidence"`
	Seconds    *int64  `json:"seconds"`
	Answer     *string `json:"answer"`
	Note       *string `json:"note"`
	EventID    int64   `json:"study_event_id"`
}

func normalizeAttempt(in *Attempt) error {
	in.Question, in.Key, in.Outcome = strings.TrimSpace(
		in.Question,
	), strings.TrimSpace(
		in.Key,
	), strings.TrimSpace(
		in.Outcome,
	)
	_, valid := scores[in.Outcome]
	if in.CourseID < 1 || in.Question == "" || utf8.RuneCountInString(in.Question) > 240 ||
		!attemptKey.MatchString(in.Key) ||
		!valid {
		return ErrInvalid
	}
	if in.Confidence != nil && (*in.Confidence < 0 || *in.Confidence > 5) ||
		in.Seconds != nil && (*in.Seconds < 0 || *in.Seconds > 86400) {
		return ErrInvalid
	}
	for _, field := range []struct {
		Value **string
		Max   int
	}{{&in.Answer, 20000}, {&in.Note, 4000}} {
		if *field.Value == nil {
			continue
		}
		if utf8.RuneCountInString(**field.Value) > field.Max {
			return ErrInvalid
		}
		if **field.Value == "" {
			*field.Value = nil
		} else {
			text := identity.Encode(**field.Value)
			*field.Value = &text
		}
	}
	return nil
}

func (s Service) Record(ctx context.Context, in Attempt) (Attempt, error) {
	if err := normalizeAttempt(&in); err != nil {
		return Attempt{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Attempt{}, err
	}
	defer tx.Rollback(ctx)
	q, err := admitQuestion(ctx, tx, in.CourseID, in.Question)
	if err != nil {
		return Attempt{}, err
	}
	in.Unit = q.Unit
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('practice-attempt:'||$1,0))`, in.Key); err != nil {
		return Attempt{}, err
	}
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT to_jsonb(a) FROM app.practice_attempts a WHERE idempotency_key=$1`, in.Key).
		Scan(&raw)
	if err == nil {
		var existing struct {
			Attempt
			Set      string `json:"set_hash"`
			Revision string `json:"blueprint_revision_hash"`
		}
		if err = json.Unmarshal(raw, &existing); err != nil {
			return Attempt{}, err
		}
		if existing.CourseID != in.CourseID || existing.Question != in.Question || existing.Outcome != in.Outcome ||
			existing.Unit != in.Unit ||
			existing.Set != q.Set ||
			existing.Revision != q.Revision ||
			!same(existing.Answer, in.Answer) ||
			!same(existing.Note, in.Note) ||
			!same(existing.Confidence, in.Confidence) ||
			!same(existing.Seconds, in.Seconds) {
			return Attempt{}, ErrConflict
		}
		in.ID, in.EventID = existing.ID, existing.EventID
	} else if err == pgx.ErrNoRows {
		if err = insertAttempt(ctx, tx, &in, q); err != nil {
			return Attempt{}, err
		}
	} else {
		return Attempt{}, err
	}
	for _, value := range []*string{in.Answer, in.Note} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	return in, tx.Commit(ctx)
}

func insertAttempt(ctx context.Context, tx pgx.Tx, in *Attempt, q currentQuestion) error {
	// Hashing the complete key avoids the legacy truncation collision for keys
	// sharing their first 119 characters.
	digest := sha256.Sum256([]byte(in.Key))
	eventKey := fmt.Sprintf("practice:%x", digest)
	err := tx.QueryRow(ctx, `INSERT INTO app.study_unit_events(course_id,plan_revision,action_id,unit_key,event_type,idempotency_key,confidence,score,note)
 VALUES($1,$2,$3,$4,'recall_answered',$5,$6,$7,$8) RETURNING id`, in.CourseID, q.Revision, in.Question, in.Unit, eventKey, in.Confidence, scores[in.Outcome], in.Note).
		Scan(&in.EventID)
	if err != nil {
		return err
	}
	return tx.QueryRow(ctx, `INSERT INTO app.practice_attempts(course_id,unit_key,question_id,question_key,set_hash,blueprint_revision_hash,outcome,grading_mode,confidence,seconds,answer,note,idempotency_key,study_event_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,'self',$8,$9,$10,$11,$12,$13) RETURNING id`,
		in.CourseID,
		in.Unit,
		in.Question,
		q.Key,
		q.Set,
		q.Revision,
		in.Outcome,
		in.Confidence,
		in.Seconds,
		in.Answer,
		in.Note,
		in.Key,
		in.EventID,
	).
		Scan(&in.ID)
}

func same[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
