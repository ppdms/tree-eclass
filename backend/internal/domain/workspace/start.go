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

func (s Service) Start(ctx context.Context, in Start) (StartResult, error) {
	if err := in.validate(); err != nil {
		return StartResult{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return StartResult{}, err
	}
	defer tx.Rollback(ctx)
	if err = tx.Workspace().LockVisibleCourse(ctx, in.CourseID); err != nil {
		return StartResult{}, err
	}
	if err = tx.Workspace().LockSessionKey(ctx, in.Key); err != nil {
		return StartResult{}, err
	}
	existing, err := tx.Workspace().SessionByKey(ctx, in.Key)
	if err == nil {
		return resumeStart(ctx, tx, in, existing.ID)
	}
	if !errors.Is(err, database.ErrNoRows) {
		return StartResult{}, err
	}
	if err = validateAction(ctx, tx, in); err != nil {
		return StartResult{}, err
	}
	return insertStart(ctx, tx, in)
}

func resumeStart(ctx context.Context, tx database.Tx, in Start, id int64) (StartResult, error) {
	session, err := lockSession(ctx, tx, id)
	if err != nil {
		return StartResult{}, err
	}
	if session.CourseID != in.CourseID || session.Action != in.Action || session.Unit != in.Unit ||
		session.Revision != in.Revision || !same(session.Planned, in.Planned) {
		return StartResult{}, ErrConflict
	}
	return startResult(ctx, tx, session)
}

func insertStart(ctx context.Context, tx database.Tx, in Start) (StartResult, error) {
	stored, err := tx.Workspace().InsertSession(ctx, database.InsertSessionParams{
		CourseID: in.CourseID,
		Action:   identity.Encode(in.Action),
		Unit:     identity.Encode(in.Unit),
		Revision: identity.Encode(in.Revision),
		Key:      in.Key,
		Planned:  in.Planned,
	})
	if err != nil {
		return StartResult{}, err
	}
	session, err := sessionFromRow(stored, nil)
	if err != nil {
		return StartResult{}, err
	}
	return startResult(ctx, tx, session)
}

// Both callers hold the session write lock: resume explicitly locks it and
// a new insert owns its row until this transaction commits.
func startResult(ctx context.Context, tx database.Tx, session Session) (StartResult, error) {
	next, err := tx.Workspace().NextBeatSequence(ctx, session.ID)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{Session: session, NextSequence: next}, tx.Commit(ctx)
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
