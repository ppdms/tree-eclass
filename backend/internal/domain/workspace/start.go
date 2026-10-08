package workspace

import (
	"context"
	"errors"

	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/navigation"
)

func (s Service) Start(ctx context.Context, in Start) (Session, error) {
	if err := in.validate(); err != nil {
		return Session{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Session{}, err
	}
	defer tx.Rollback(ctx)
	if err = tx.Workspace().LockVisibleCourse(ctx, in.CourseID); err != nil {
		return Session{}, err
	}
	if err = tx.Workspace().LockSessionKey(ctx, in.Key); err != nil {
		return Session{}, err
	}
	existing, err := sessionFromRow(tx.Workspace().SessionByKey(ctx, in.Key))
	if err == nil {
		if existing.CourseID != in.CourseID || existing.Action != in.Action || existing.Unit != in.Unit ||
			existing.Revision != in.Revision ||
			!same(existing.Planned, in.Planned) {
			return Session{}, ErrConflict
		}
		return existing, tx.Commit(ctx)
	}
	if !errors.Is(err, database.ErrNoRows) {
		return Session{}, err
	}
	if err = validateAction(ctx, tx, in); err != nil {
		return Session{}, err
	}
	stored, err := tx.Workspace().InsertSession(ctx, database.InsertSessionParams{
		CourseID: in.CourseID,
		Action:   identity.Encode(in.Action),
		Unit:     identity.Encode(in.Unit),
		Revision: identity.Encode(in.Revision),
		Key:      in.Key,
		Planned:  in.Planned,
	})
	if err != nil {
		return Session{}, err
	}
	result, err := sessionFromRow(stored, nil)
	if err != nil {
		return Session{}, err
	}
	return result, tx.Commit(ctx)
}

func (in *Start) validate() error {
	in.Key = strings.TrimSpace(in.Key)
	if in.CourseID < 1 || !keyPattern.MatchString(in.Key) ||
		(in.Planned != nil && (*in.Planned < 0 || *in.Planned > 1440)) {
		return ErrInvalid
	}
	if utf8.RuneCountInString(in.Action) > 240 || utf8.RuneCountInString(in.Unit) > 160 ||
		utf8.RuneCountInString(in.Revision) > 128 {
		return ErrInvalid
	}
	return nil
}

func validateAction(ctx context.Context, tx database.Tx, in Start) error {
	if in.Action == "" {
		if in.Unit != "" || in.Revision != "" {
			return ErrInvalid
		}
		return nil
	}
	unit, err := navigation.AdmitAction(ctx, tx, in.CourseID, in.Action, in.Revision)
	if err == navigation.ErrActionConflict {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if unit != in.Unit {
		return ErrConflict
	}
	return nil
}
