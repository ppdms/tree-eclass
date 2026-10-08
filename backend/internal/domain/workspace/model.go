// Package workspace owns the transactional reader attention and outcome ledger.
package workspace

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool database.Store }

var ErrConflict = errors.New("the study request conflicts with existing state")
var ErrInvalid = errors.New("invalid study session fields")
var ErrDocumentPending = errors.New("this document is still being prepared for study")
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

func sessionFromRow(stored database.WorkspaceSession, err error) (Session, error) {
	if err != nil {
		return Session{}, err
	}
	result := Session{
		ID:         stored.ID,
		CourseID:   stored.CourseID,
		Action:     identity.Decode(stored.Action),
		Unit:       identity.Decode(stored.Unit),
		Revision:   identity.Decode(stored.Revision),
		Key:        stored.Key,
		Planned:    stored.Planned,
		Active:     stored.Active,
		Visible:    stored.Visible,
		Outcome:    stored.Outcome,
		Started:    stored.Started,
		Seen:       stored.Seen,
		Ended:      stored.Ended,
		Confidence: stored.Confidence,
		EventID:    stored.EventID,
	}
	if stored.Note != nil {
		note := identity.Decode(*stored.Note)
		result.Note = &note
	}
	return result, nil
}

func lockSession(ctx context.Context, tx database.Tx, id int64) (Session, error) {
	return sessionFromRow(tx.Workspace().LockSession(ctx, id))
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
