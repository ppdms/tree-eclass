// Package workspace owns the transactional reader attention and outcome ledger.
package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool *pgxpool.Pool }

var ErrConflict = errors.New("the study request conflicts with existing state")
var ErrInvalid = errors.New("invalid study session fields")
var keyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type Session struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	Action     string  `json:"action_id"`
	Unit       string  `json:"unit_key"`
	Revision   string  `json:"plan_revision"`
	Key        string  `json:"client_session_key"`
	Planned    *int64  `json:"planned_minutes"`
	Active     int64   `json:"active_seconds"`
	Visible    int64   `json:"visible_seconds"`
	Outcome    *string `json:"outcome"`
	Note       *string `json:"note"`
	Started    *string `json:"started_at"`
	Seen       *string `json:"last_seen_at"`
	Ended      *string `json:"ended_at"`
	Confidence *int64  `json:"confidence"`
	EventID    *int64  `json:"study_event_id"`
}
type Start struct {
	CourseID                    int64
	Key, Action, Unit, Revision string
	Planned                     *int64
}

func sessionRow(row pgx.Row) (Session, error) {
	var raw []byte
	var result Session
	if err := row.Scan(&raw); err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	for _, value := range []*string{&result.Action, &result.Unit, &result.Revision, result.Note} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	return result, nil
}

func lockSession(ctx context.Context, tx pgx.Tx, id int64) (Session, error) {
	// Serialize against course hiding/deletion before locking the session, in
	// the same order as the rest of the learner mutations.
	var course int64
	if err := tx.QueryRow(ctx, `SELECT c.id FROM app.courses c JOIN app.study_workspace_sessions s ON s.course_id=c.id WHERE s.id=$1 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1)) FOR SHARE OF c`, id).Scan(&course); err != nil {
		return Session{}, err
	}
	return sessionRow(
		tx.QueryRow(ctx, `SELECT to_jsonb(s) FROM app.study_workspace_sessions s WHERE s.id=$1 FOR UPDATE`, id),
	)
}

func cleanNote(note *string) (*string, error) {
	if note == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*note)
	if utf8.RuneCountInString(value) > 4000 {
		return nil, ErrInvalid
	}
	if value == "" {
		return nil, nil
	}
	return &value, nil
}
func same[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
