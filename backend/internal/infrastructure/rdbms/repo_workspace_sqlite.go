package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteWorkspace struct{ db nativeDBTX }

const workspaceSessionColumnsSQLite = `id,course_id,action_id,unit_key,plan_revision,client_session_key,` +
	`planned_minutes,active_seconds,visible_seconds,outcome,note,started_at,last_seen_at,ended_at,` +
	`confidence,study_event_id`

func scanWorkspaceSessionSQLite(row nativeRow) (database.WorkspaceSession, error) {
	var session database.WorkspaceSession
	err := row.Scan(
		&session.ID,
		&session.CourseID,
		&session.Action,
		&session.Unit,
		&session.Revision,
		&session.Key,
		&session.Planned,
		&session.Active,
		&session.Visible,
		&session.Outcome,
		&session.Note,
		&session.Started,
		&session.Seen,
		&session.Ended,
		&session.Confidence,
		&session.EventID,
	)
	return session, err
}

func (w sqliteWorkspace) LockVisibleCourse(ctx context.Context, course int64) error {
	// No row locks on SQLite: the admitted writer owns every write until
	// commit/rollback, which is stronger than FOR SHARE. The guard still
	// verifies study visibility in the same snapshot as the session write.
	var id int64
	return w.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`+
		` AND (hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p`+
		` WHERE p.course_id=courses.id AND p.enabled=1))`, course).Scan(&id)
}

func (w sqliteWorkspace) CheckVisibleCourse(ctx context.Context, course int64) error {
	var id int64
	return w.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`+
		` AND (hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p`+
		` WHERE p.course_id=courses.id AND p.enabled=1))`, course).Scan(&id)
}

func (w sqliteWorkspace) LockSessionKey(ctx context.Context, key string) error {
	// The admitted writer owns every write until commit/rollback;
	// requires a writer transaction. The key is accepted for parity.
	_, err := advisoryLock(ctx, w.db, "workspace:"+key, true, false)
	return err
}

func (w sqliteWorkspace) SessionByKey(ctx context.Context, key string) (database.WorkspaceSession, error) {
	return scanWorkspaceSessionSQLite(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsSQLite+` FROM study_workspace_sessions WHERE client_session_key=?`, key))
}

func (w sqliteWorkspace) SessionByID(ctx context.Context, id int64) (database.WorkspaceSession, error) {
	return scanWorkspaceSessionSQLite(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsSQLite+` FROM study_workspace_sessions WHERE id=?`, id))
}

func (w sqliteWorkspace) LockSession(ctx context.Context, id int64) (database.WorkspaceSession, error) {
	var course int64
	err := w.db.QueryRow(ctx, `SELECT c.id FROM courses c`+
		` JOIN study_workspace_sessions s ON s.course_id=c.id`+
		` WHERE s.id=? AND (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p`+
		` WHERE p.course_id=c.id AND p.enabled=1))`, id).Scan(&course)
	if err != nil {
		return database.WorkspaceSession{}, err
	}
	return scanWorkspaceSessionSQLite(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsSQLite+` FROM study_workspace_sessions WHERE id=?`, id))
}

func (w sqliteWorkspace) InsertSession(
	ctx context.Context,
	params database.InsertSessionParams,
) (database.WorkspaceSession, error) {
	return scanWorkspaceSessionSQLite(w.db.QueryRow(ctx,
		`INSERT INTO study_workspace_sessions(course_id,action_id,unit_key,plan_revision,`+
			`client_session_key,planned_minutes,last_seen_at)`+
			` VALUES(?,?,?,?,?,?,strftime('%Y-%m-%d %H:%M:%S','now'))`+
			` RETURNING `+workspaceSessionColumnsSQLite,
		params.CourseID, params.Action, params.Unit, params.Revision, params.Key, params.Planned))
}

func (w sqliteWorkspace) BeatHash(ctx context.Context, session, sequence int64) (*string, error) {
	var hash *string
	err := w.db.QueryRow(ctx, `SELECT request_hash FROM study_reading_beats`+
		` WHERE session_id=? AND sequence=?`, session, sequence).Scan(&hash)
	return hash, err
}

func (w sqliteWorkspace) InsertBeat(ctx context.Context, session, sequence int64, hash string) error {
	_, err := w.db.Exec(ctx, `INSERT INTO study_reading_beats(session_id,sequence,request_hash)`+
		` VALUES(?,?,?)`, session, sequence, hash)
	return err
}

func (w sqliteWorkspace) ReadyDocument(ctx context.Context, course int64, document string) (string, *int64, error) {
	var hash string
	var pages *int64
	err := w.db.QueryRow(ctx, `SELECT source_hash,page_count FROM documents`+
		` WHERE id=? AND course_id=? AND is_current=1 AND status='ready'`,
		document, course).Scan(&hash, &pages)
	return hash, pages, err
}

func (w sqliteWorkspace) AccumulateSpan(ctx context.Context, params database.AccumulateSpanParams) error {
	_, err := w.db.Exec(ctx, `INSERT INTO study_reading_spans(session_id,course_id,document_id,`+
		`source_hash,page_number,action_id,unit_key,plan_revision,active_seconds,visible_seconds,ended_at)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,strftime('%Y-%m-%d %H:%M:%S','now'))`+
		` ON CONFLICT(session_id,document_id,source_hash,page_number) DO UPDATE SET`+
		` active_seconds=study_reading_spans.active_seconds+excluded.active_seconds,`+
		`visible_seconds=study_reading_spans.visible_seconds+excluded.visible_seconds,`+
		`ended_at=excluded.ended_at`,
		params.SessionID, params.CourseID, params.Document, params.SourceHash, params.Page,
		params.Action, params.Unit, params.Revision, params.Active, params.Visible)
	return err
}

func (w sqliteWorkspace) AddAttention(ctx context.Context, session, active, visible int64) error {
	_, err := w.db.Exec(ctx, `UPDATE study_workspace_sessions`+
		` SET active_seconds=active_seconds+?,visible_seconds=visible_seconds+?,`+
		`last_seen_at=strftime('%Y-%m-%d %H:%M:%S','now') WHERE id=?`,
		active, visible, session)
	return err
}

func (w sqliteWorkspace) ReadingTotals(
	ctx context.Context,
	course int64,
	action, document string,
) ([]database.ReadingPage, error) {
	rows, err := w.db.Query(ctx, `SELECT document_id,page_number,`+
		`sum(active_seconds),sum(visible_seconds) FROM study_reading_spans`+
		` WHERE course_id=? AND (?='' OR action_id=?) AND (?='' OR document_id=?)`+
		` GROUP BY document_id,page_number ORDER BY document_id,page_number`,
		course, action, action, document, document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ReadingPage{}
	for rows.Next() {
		var page database.ReadingPage
		if err := rows.Scan(&page.Document, &page.Page, &page.Active, &page.Visible); err != nil {
			return nil, err
		}
		out = append(out, page)
	}
	return out, rows.Err()
}

func (w sqliteWorkspace) WorkspaceDocument(
	ctx context.Context, course int64, id string,
) (database.WorkspaceDocument, error) {
	var doc database.WorkspaceDocument
	err := w.db.QueryRow(ctx, `SELECT display_name,source_path,source_origin,source_hash,document_kind,`+
		`mime_type,coalesce(page_count,0),reading_minutes,language_hint FROM documents d`+
		` WHERE id=? AND course_id=? AND is_current=1 AND status='ready'`+
		` AND EXISTS(SELECT 1 FROM document_revisions r JOIN objects o ON o.id=r.object_id`+
		` WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.deleted_at IS NULL AND o.sha256=d.source_hash)`,
		id, course).Scan(
		&doc.DisplayName, &doc.SourcePath, &doc.SourceOrigin, &doc.SourceHash, &doc.DocumentKind,
		&doc.MimeType, &doc.PageCount, &doc.ReadingMinutes, &doc.LanguageHint)
	return doc, err
}

func (w sqliteWorkspace) DocumentPending(ctx context.Context, course int64, id string) (bool, error) {
	var pending bool
	err := w.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents`+
		` WHERE id=? AND course_id=? AND is_current=1 AND status IN('pending','running'))`,
		id, course).Scan(&pending)
	return pending, err
}

func (w sqliteWorkspace) DocumentInsights(
	ctx context.Context,
	params database.DocumentInsightParams,
) (database.DocumentInsightCounts, error) {
	var counts database.DocumentInsightCounts
	err := w.db.QueryRow(ctx, `SELECT`+
		` (SELECT count(*) FROM page_enrichments`+
		` WHERE document_id=? AND source_hash=? AND analysis_version=? AND requested_model=? AND status='ready'),`+
		` (SELECT count(*) FROM study_annotations`+
		` WHERE document_id=? AND course_id=? AND status<>'deleted'),`+
		` (SELECT count(*) FROM study_annotations`+
		` WHERE document_id=? AND course_id=? AND status<>'deleted'`+
		` AND (status='orphaned' OR source_hash<>?))`,
		params.Document, params.SourceHash, params.AnalysisVersion, params.RequestedModel,
		params.Document, params.CourseID,
		params.Document, params.CourseID, params.SourceHash).Scan(
		&counts.ReadyPages, &counts.Annotations, &counts.Orphaned)
	return counts, err
}

func (w sqliteWorkspace) CloseSession(
	ctx context.Context,
	params database.CloseSessionParams,
) (database.WorkspaceSession, error) {
	return scanWorkspaceSessionSQLite(w.db.QueryRow(ctx,
		`UPDATE study_workspace_sessions SET outcome=?,note=?,confidence=?,study_event_id=?,`+
			`ended_at=strftime('%Y-%m-%d %H:%M:%S','now')`+
			` WHERE id=? RETURNING `+workspaceSessionColumnsSQLite,
		params.Outcome, params.Note, params.Confidence, params.EventID, params.ID))
}
