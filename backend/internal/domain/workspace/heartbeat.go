package workspace

import (
	"context"
	"errors"

	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Beat struct {
	SessionID, Sequence, Page, Interval int64
	Document                            string
	Active                              bool
}
type BeatResult struct {
	Status  string  `json:"status"`
	Session Session `json:"session"`
}

func (s Service) Heartbeat(ctx context.Context, in Beat) (BeatResult, error) {
	result := BeatResult{Status: "recorded"}
	if err := validateBeat(in); err != nil {
		return result, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result.Session, err = lockSession(ctx, tx, in.SessionID)
	if err != nil {
		return result, err
	}
	raw, _ := json.Marshal(in)
	hash := sha256.Sum256(raw)
	requestHash := hex.EncodeToString(hash[:])
	previous, err := tx.Workspace().BeatHash(ctx, in.SessionID, in.Sequence)
	if err == nil {
		if previous != nil && *previous != requestHash {
			return result, ErrConflict
		}
		result.Status = "duplicate"
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, database.ErrNoRows) {
		return result, err
	}
	if result.Session.Ended != nil {
		return result, ErrConflict
	}
	result.Session, err = recordBeat(ctx, tx, result.Session, in, requestHash)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func recordBeat(
	ctx context.Context,
	tx database.Tx,
	session Session,
	in Beat,
	requestHash string,
) (Session, error) {
	source, pages, err := tx.Workspace().ReadyDocument(ctx, session.CourseID, in.Document)
	if err != nil {
		return Session{}, err
	}
	if pages != nil && *pages > 0 && in.Page > *pages {
		return Session{}, ErrInvalid
	}
	if err = tx.Workspace().InsertBeat(ctx, in.SessionID, in.Sequence, requestHash); err != nil {
		return Session{}, err
	}
	if err = accumulate(ctx, tx, session, in, source); err != nil {
		return Session{}, err
	}
	return sessionFromRow(tx.Workspace().SessionByID(ctx, in.SessionID))
}

func validateBeat(in Beat) error {
	if in.SessionID < 1 || in.Sequence < 0 || in.Sequence > 1<<31 || in.Page < 1 || in.Page > 100000 ||
		in.Interval < 0 ||
		in.Interval > 3600 ||
		in.Document == "" ||
		len(in.Document) > 200 {
		return ErrInvalid
	}
	return nil
}

func accumulate(ctx context.Context, tx database.Tx, session Session, in Beat, source string) error {
	interval := min(in.Interval, 90)
	active := int64(0)
	if in.Active {
		active = interval
	}
	err := tx.Workspace().AccumulateSpan(ctx, database.AccumulateSpanParams{
		SessionID:  session.ID,
		CourseID:   session.CourseID,
		Document:   in.Document,
		SourceHash: source,
		Page:       in.Page,
		Action:     identity.Encode(session.Action),
		Unit:       identity.Encode(session.Unit),
		Revision:   identity.Encode(session.Revision),
		Active:     active,
		Visible:    interval,
	})
	if err != nil {
		return err
	}
	return tx.Workspace().AddAttention(ctx, session.ID, active, interval)
}
