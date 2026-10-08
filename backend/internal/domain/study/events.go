package study

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/navigation"
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
	if err = tx.Study().LockCourseForEvent(ctx, in.CourseID); err != nil {
		return Event{}, err
	}
	in.Unit, err = navigation.AdmitAction(ctx, tx, in.CourseID, in.Action, in.Revision)
	if err != nil {
		return Event{}, err
	}
	if err = tx.Study().LockEventKey(ctx, in.Key); err != nil {
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

func storeEvent(ctx context.Context, ops database.Operations, in Event) (int64, error) {
	existing, err := ops.Study().StudyEventByKey(ctx, in.Key)
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
	} else if errors.Is(err, database.ErrNoRows) {
		return ops.Study().InsertStudyEvent(ctx, database.StudyEventParams{
			CourseID:   in.CourseID,
			Action:     identity.Encode(in.Action),
			Revision:   identity.Encode(in.Revision),
			Unit:       identity.Encode(in.Unit),
			Type:       in.Type,
			Key:        in.Key,
			Confidence: in.Confidence,
			Minutes:    in.Minutes,
			Note:       in.Note,
		})
	}
	return 0, err
}

func equal[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
