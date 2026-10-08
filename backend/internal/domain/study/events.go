package study

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/infrastructure/rdbms"
)

var ErrInvalidEvent = errors.New("invalid study event")
var ErrEventConflict = errors.New("study event key was already used differently")
var eventKey = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type Event struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	Action     string  `json:"action_id"`
	Revision   string  `json:"plan_revision"`
	Unit       string  `json:"unit_key"`
	Type       string  `json:"event_type"`
	Key        string  `json:"idempotency_key"`
	Confidence *int64  `json:"confidence"`
	Minutes    *int64  `json:"actual_minutes"`
	Note       *string `json:"note"`
}

func normalizeEvent(in *Event) error {
	in.Action, in.Revision, in.Key, in.Type = strings.TrimSpace(
		in.Action,
	), strings.TrimSpace(
		in.Revision,
	), strings.TrimSpace(
		in.Key,
	), strings.TrimSpace(
		in.Type,
	)
	if in.CourseID < 1 || !eventKey.MatchString(in.Key) || in.Action == "" || utf8.RuneCountInString(in.Action) > 240 ||
		in.Revision == "" ||
		utf8.RuneCountInString(in.Revision) > 128 {
		return ErrInvalidEvent
	}
	if !slices.Contains([]string{"started", "completed", "partial", "stuck", "deferred"}, in.Type) {
		return ErrInvalidEvent
	}
	if in.Minutes != nil && (*in.Minutes < 0 || *in.Minutes > 1440) ||
		in.Confidence != nil && (*in.Confidence < 0 || *in.Confidence > 5) {
		return ErrInvalidEvent
	}
	if in.Type == "partial" && (in.Minutes == nil || *in.Minutes == 0) {
		return ErrInvalidEvent
	}
	if in.Note != nil {
		note := strings.TrimSpace(*in.Note)
		if utf8.RuneCountInString(note) > 4000 {
			return ErrInvalidEvent
		}
		in.Note = nil
		if note != "" {
			note = identity.Encode(note)
			in.Note = &note
		}
	}
	return nil
}

func (s Service) Record(ctx context.Context, in Event) (Event, error) {
	if err := normalizeEvent(&in); err != nil {
		return Event{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx)
	var course int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=app.courses.id AND p.enabled=1)) FOR SHARE`, in.CourseID).Scan(&course); err != nil {
		return Event{}, err
	}
	in.Unit, err = navigation.AdmitAction(ctx, tx, in.CourseID, in.Action, in.Revision)
	if err != nil {
		return Event{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('study-event:'||$1,0))`, in.Key); err != nil {
		return Event{}, err
	}
	in.ID, err = storeEvent(ctx, tx, in)
	if err != nil {
		return Event{}, err
	}
	if in.Note != nil {
		note := identity.Decode(*in.Note)
		in.Note = &note
	}
	return in, tx.Commit(ctx)
}

func storeEvent(ctx context.Context, tx rdbms.Tx, in Event) (int64, error) {
	var existing Event
	err := tx.QueryRow(ctx, `SELECT id,course_id,action_id,plan_revision,unit_key,event_type,confidence,actual_minutes,note FROM app.study_unit_events WHERE idempotency_key=$1`, in.Key).
		Scan(
			&existing.ID,
			&existing.CourseID,
			&existing.Action,
			&existing.Revision,
			&existing.Unit,
			&existing.Type,
			&existing.Confidence,
			&existing.Minutes,
			&existing.Note,
		)
	if err == nil {
		if existing.CourseID != in.CourseID || identity.Decode(existing.Action) != in.Action ||
			identity.Decode(existing.Revision) != in.Revision ||
			identity.Decode(existing.Unit) != in.Unit ||
			existing.Type != in.Type ||
			!equal(existing.Note, in.Note) ||
			!equal(existing.Confidence, in.Confidence) ||
			!equal(existing.Minutes, in.Minutes) {
			return 0, ErrEventConflict
		}
		return existing.ID, nil
	} else if errors.Is(err, rdbms.ErrNoRows) {
		err = tx.QueryRow(
			ctx,
			`INSERT INTO app.study_unit_events(course_id,action_id,plan_revision,unit_key,event_type,idempotency_key,confidence,actual_minutes,note) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
			in.CourseID,
			identity.Encode(in.Action),
			identity.Encode(in.Revision),
			identity.Encode(in.Unit),
			in.Type,
			in.Key,
			in.Confidence,
			in.Minutes,
			in.Note,
		).Scan(&in.ID)
		if err != nil {
			return 0, err
		}
		return in.ID, nil
	}
	return 0, err
}

func equal[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
