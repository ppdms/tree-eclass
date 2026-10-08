package workspace

import (
	"context"
	"errors"

	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/infrastructure/rdbms"
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
	var id int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=app.courses.id AND p.enabled=1)) FOR SHARE`, in.CourseID).Scan(&id); err != nil {
		return Session{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('workspace:'||$1,0))`, in.Key); err != nil {
		return Session{}, err
	}
	existing, err := sessionRow(
		tx.QueryRow(ctx, `SELECT `+sessionColumns+` FROM app.study_workspace_sessions s WHERE client_session_key=$1`, in.Key),
	)
	if err == nil {
		if existing.CourseID != in.CourseID || existing.Action != in.Action || existing.Unit != in.Unit ||
			existing.Revision != in.Revision ||
			!same(existing.Planned, in.Planned) {
			return Session{}, ErrConflict
		}
		return existing, tx.Commit(ctx)
	}
	if !errors.Is(err, rdbms.ErrNoRows) {
		return Session{}, err
	}
	if err = validateAction(ctx, tx, in); err != nil {
		return Session{}, err
	}
	row := tx.QueryRow(
		ctx,
		`INSERT INTO app.study_workspace_sessions(course_id,action_id,unit_key,plan_revision,client_session_key,planned_minutes,last_seen_at)
 VALUES($1,$2,$3,$4,$5,$6,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')) RETURNING `+sessionColumns,
		in.CourseID,
		identity.Encode(in.Action),
		identity.Encode(in.Unit),
		identity.Encode(in.Revision),
		in.Key,
		in.Planned,
	)
	result, err := sessionRow(row)
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

func validateAction(ctx context.Context, tx rdbms.Tx, in Start) error {
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
