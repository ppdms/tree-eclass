package workspace

import (
	"context"
	"fmt"
	"slices"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Finish struct {
	SessionID  int64
	Outcome    string
	Note       *string
	Confidence *int64
}
type FinishResult struct {
	Status  string  `json:"status"`
	Session Session `json:"session"`
	Minutes int64   `json:"measured_minutes"`
	EventID *int64  `json:"study_event_id"`
	Reading Reading `json:"reading"`
}

func (s Service) Finish(ctx context.Context, in Finish) (FinishResult, error) {
	result := FinishResult{Status: "closed"}
	if in.SessionID < 1 ||
		!slices.Contains([]string{"completed", "partial", "stuck", "deferred", "abandoned"}, in.Outcome) ||
		(in.Confidence != nil && (*in.Confidence < 0 || *in.Confidence > 5)) {
		return result, ErrInvalid
	}
	note, err := cleanNote(in.Note)
	if err != nil {
		return result, err
	}
	in.Note = note
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result.Session, err = lockSession(ctx, tx, in.SessionID)
	if err != nil {
		return result, err
	}
	if result.Session.Ended != nil {
		if result.Session.Outcome == nil || *result.Session.Outcome != in.Outcome ||
			!same(result.Session.Note, in.Note) ||
			!same(result.Session.Confidence, in.Confidence) {
			return result, ErrConflict
		}
	} else {
		result.Session, err = closeSession(ctx, tx, result.Session, in)
		if err != nil {
			return result, err
		}
	}
	result.EventID = result.Session.EventID
	result.Minutes = result.Session.Active / 60
	result.Reading, err = readingTotals(ctx, tx, result.Session.CourseID, result.Session.Action, "")
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func closeSession(ctx context.Context, tx database.Tx, session Session, in Finish) (Session, error) {
	var event *int64
	var note *string
	if in.Note != nil {
		text := identity.Encode(*in.Note)
		note = &text
	}
	if in.Outcome != "abandoned" && session.Action != "" && session.Revision != "" {
		kind := in.Outcome
		minutes := session.Active / 60
		if kind == "partial" && minutes == 0 {
			kind = "deferred"
		}
		var measured *int64
		if minutes > 0 {
			measured = &minutes
		}
		id, err := tx.Study().InsertStudyEvent(ctx, database.StudyEventParams{
			CourseID:   session.CourseID,
			Action:     identity.Encode(session.Action),
			Revision:   identity.Encode(session.Revision),
			Unit:       identity.Encode(session.Unit),
			Type:       kind,
			Key:        fmt.Sprintf("workspace:%d:%s", session.ID, kind),
			Confidence: in.Confidence,
			Minutes:    measured,
			Note:       note,
		})
		if err != nil {
			return session, err
		}
		event = &id
	}
	return sessionFromRow(tx.Workspace().CloseSession(ctx, database.CloseSessionParams{
		ID:         session.ID,
		Outcome:    in.Outcome,
		Note:       note,
		Confidence: in.Confidence,
		EventID:    event,
	}))
}
