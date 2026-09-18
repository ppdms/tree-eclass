package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
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
	var previous *string
	err = tx.QueryRow(ctx, `SELECT request_hash FROM app.study_reading_beats WHERE session_id=$1 AND sequence=$2`, in.SessionID, in.Sequence).
		Scan(&previous)
	if err == nil {
		if previous != nil && *previous != requestHash {
			return result, ErrConflict
		}
		result.Status = "duplicate"
		return result, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return result, err
	}
	if result.Session.Ended != nil {
		return result, ErrConflict
	}
	result.Session, err = s.recordBeat(ctx, tx, result.Session, in, requestHash)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func (s Service) recordBeat(
	ctx context.Context,
	tx pgx.Tx,
	session Session,
	in Beat,
	requestHash string,
) (Session, error) {
	var source string
	var pages *int64
	err := tx.QueryRow(ctx, `SELECT source_hash,page_count FROM knowledge.documents WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready' FOR SHARE`, in.Document, session.CourseID).
		Scan(&source, &pages)
	if err != nil {
		return Session{}, err
	}
	if pages != nil && *pages > 0 && in.Page > *pages {
		return Session{}, ErrInvalid
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app.study_reading_beats(session_id,sequence,request_hash) VALUES($1,$2,$3)`, in.SessionID, in.Sequence, requestHash); err != nil {
		return Session{}, err
	}
	if err = accumulate(ctx, tx, session, in, source); err != nil {
		return Session{}, err
	}
	return sessionRow(
		tx.QueryRow(ctx, `SELECT to_jsonb(s) FROM app.study_workspace_sessions s WHERE id=$1`, in.SessionID),
	)
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

func accumulate(ctx context.Context, tx pgx.Tx, session Session, in Beat, source string) error {
	interval := min(in.Interval, 90)
	active := int64(0)
	if in.Active {
		active = interval
	}
	_, err := tx.Exec(
		ctx,
		`INSERT INTO app.study_reading_spans(session_id,course_id,document_id,source_hash,page_number,action_id,unit_key,plan_revision,active_seconds,visible_seconds,ended_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))
 ON CONFLICT ON CONSTRAINT reading_span_revision DO UPDATE SET active_seconds=study_reading_spans.active_seconds+excluded.active_seconds,visible_seconds=study_reading_spans.visible_seconds+excluded.visible_seconds,ended_at=excluded.ended_at`,
		session.ID,
		session.CourseID,
		in.Document,
		source,
		in.Page,
		identity.Encode(session.Action),
		identity.Encode(session.Unit),
		identity.Encode(session.Revision),
		active,
		interval,
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE app.study_workspace_sessions SET active_seconds=active_seconds+$2,visible_seconds=visible_seconds+$3,last_seen_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$1`,
		session.ID,
		active,
		interval,
	)
	return err
}
