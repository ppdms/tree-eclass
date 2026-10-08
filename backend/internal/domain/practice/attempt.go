package practice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
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
	found, err := loadExistingAttempt(ctx, tx, &in, q)
	if err != nil {
		return Attempt{}, err
	}
	if !found {
		if err = insertAttempt(ctx, tx, &in, q); err != nil {
			return Attempt{}, err
		}
	}
	decodeAttemptText(&in)
	return in, tx.Commit(ctx)
}

// loadExistingAttempt fills in.ID/in.EventID from the stored idempotent row
// and reports whether one exists. A stored row whose fields disagree with the
// request is a conflicting retry.
func loadExistingAttempt(ctx context.Context, tx database.Tx, in *Attempt, q currentQuestion) (bool, error) {
	stored, err := tx.Practice().AttemptByKey(ctx, in.Key)
	if database.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existing := storedAttempt(stored)
	decodeAttemptText(&existing)
	if attemptConflict(existing, *in, q, stored.Set, stored.Revision) {
		return false, ErrConflict
	}
	in.ID, in.EventID = stored.ID, stored.EventID
	return true, nil
}

// storedAttempt projects the neutral attempt row onto the domain shape for
// conflict comparison. Answer/Note stay encoded until decodeAttemptText.
func storedAttempt(stored database.PracticeAttemptRow) Attempt {
	return Attempt{
		ID: stored.ID, CourseID: stored.CourseID, Unit: stored.Unit, Question: stored.Question,
		Outcome: stored.Outcome, Key: stored.Key, Confidence: stored.Confidence,
		Seconds: stored.Seconds, Answer: stored.Answer, Note: stored.Note, EventID: stored.EventID,
	}
}

// attemptConflict reports whether the stored idempotent row disagrees with
// the incoming request on identity, outcome, fingerprints, or payload.
func attemptConflict(existing, in Attempt, q currentQuestion, set, revision string) bool {
	return existing.CourseID != in.CourseID || existing.Question != in.Question || existing.Outcome != in.Outcome ||
		existing.Unit != in.Unit ||
		set != q.Set ||
		revision != q.Revision ||
		!same(existing.Answer, in.Answer) ||
		!same(existing.Note, in.Note) ||
		!same(existing.Confidence, in.Confidence) ||
		!same(existing.Seconds, in.Seconds)
}

// decodeAttemptText decodes stored identity-encoded text fields in place.
func decodeAttemptText(in *Attempt) {
	for _, value := range []*string{in.Answer, in.Note} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
}

func insertAttempt(ctx context.Context, tx database.Tx, in *Attempt, q currentQuestion) error {
	// Hashing the complete key avoids the legacy truncation collision for keys
	// sharing their first 119 characters.
	digest := sha256.Sum256([]byte(in.Key))
	eventKey := fmt.Sprintf("practice:%x", digest)
	attemptID, eventID, err := tx.Practice().InsertAttempt(ctx, database.PracticeAttemptParams{
		CourseID: in.CourseID, Unit: in.Unit, Question: in.Question,
		Key: q.Key, Set: q.Set, Revision: q.Revision, Outcome: in.Outcome,
		Confidence: in.Confidence, Seconds: in.Seconds, Score: scores[in.Outcome],
		Answer: in.Answer, Note: in.Note, IdemKey: in.Key, EventKey: eventKey,
	})
	if err != nil {
		return err
	}
	in.ID, in.EventID = attemptID, eventID
	return nil
}

func same[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
