package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresWorkspace struct{ db nativeDBTX }

const workspaceSessionColumnsPG = `id,course_id,action_id,unit_key,plan_revision,client_session_key,` +
	`planned_minutes,active_seconds,visible_seconds,outcome,note,started_at,last_seen_at,ended_at,` +
	`confidence,study_event_id`

func scanWorkspaceSession(row nativeRow) (database.WorkspaceSession, error) {
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

func (w postgresWorkspace) LockVisibleCourse(ctx context.Context, course int64) error {
	var id int64
	return w.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1
	AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
	WHERE p.course_id=app.courses.id AND p.enabled=1)) FOR SHARE`, course).Scan(&id)
}

func (w postgresWorkspace) CheckVisibleCourse(ctx context.Context, course int64) error {
	var id int64
	return w.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1
	AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
	WHERE p.course_id=app.courses.id AND p.enabled=1))`, course).Scan(&id)
}

func (w postgresWorkspace) LockSessionKey(ctx context.Context, key string) error {
	_, err := advisoryLock(ctx, w.db, "workspace:"+key, true, false)
	return err
}

func (w postgresWorkspace) SessionByKey(ctx context.Context, key string) (database.WorkspaceSession, error) {
	return scanWorkspaceSession(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsPG+` FROM app.study_workspace_sessions WHERE client_session_key=$1`, key))
}

func (w postgresWorkspace) SessionByID(ctx context.Context, id int64) (database.WorkspaceSession, error) {
	return scanWorkspaceSession(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsPG+` FROM app.study_workspace_sessions WHERE id=$1`, id))
}

func (w postgresWorkspace) LockSession(ctx context.Context, id int64) (database.WorkspaceSession, error) {
	var course int64
	err := w.db.QueryRow(ctx, `SELECT c.id FROM app.courses c
	JOIN app.study_workspace_sessions s ON s.course_id=c.id
	WHERE s.id=$1 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
	WHERE p.course_id=c.id AND p.enabled=1)) FOR SHARE OF c`, id).Scan(&course)
	if err != nil {
		return database.WorkspaceSession{}, err
	}
	return scanWorkspaceSession(w.db.QueryRow(ctx,
		`SELECT `+workspaceSessionColumnsPG+` FROM app.study_workspace_sessions WHERE id=$1 FOR UPDATE`, id))
}

func (w postgresWorkspace) InsertSession(
	ctx context.Context,
	params database.InsertSessionParams,
) (database.WorkspaceSession, error) {
	return scanWorkspaceSession(w.db.QueryRow(ctx,
		`INSERT INTO app.study_workspace_sessions(course_id,action_id,unit_key,plan_revision,`+
			`client_session_key,planned_minutes,last_seen_at)`+
			` VALUES($1,$2,$3,$4,$5,$6,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))`+
			` RETURNING `+workspaceSessionColumnsPG,
		params.CourseID, params.Action, params.Unit, params.Revision, params.Key, params.Planned))
}

func (w postgresWorkspace) BeatHash(ctx context.Context, session, sequence int64) (*string, error) {
	var hash *string
	err := w.db.QueryRow(ctx, `SELECT request_hash FROM app.study_reading_beats`+
		` WHERE session_id=$1 AND sequence=$2`, session, sequence).Scan(&hash)
	return hash, err
}

func (w postgresWorkspace) InsertBeat(ctx context.Context, session, sequence int64, hash string) error {
	_, err := w.db.Exec(ctx, `INSERT INTO app.study_reading_beats(session_id,sequence,request_hash)`+
		` VALUES($1,$2,$3)`, session, sequence, hash)
	return err
}

func (w postgresWorkspace) ReadyDocument(ctx context.Context, course int64, document string) (string, *int64, error) {
	var hash string
	var pages *int64
	err := w.db.QueryRow(ctx, `SELECT source_hash,page_count FROM knowledge.documents`+
		` WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready' FOR SHARE`,
		document, course).Scan(&hash, &pages)
	return hash, pages, err
}

func (w postgresWorkspace) AccumulateSpan(ctx context.Context, params database.AccumulateSpanParams) error {
	_, err := w.db.Exec(ctx, `INSERT INTO app.study_reading_spans(session_id,course_id,document_id,`+
		`source_hash,page_number,action_id,unit_key,plan_revision,active_seconds,visible_seconds,ended_at)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))`+
		` ON CONFLICT(session_id,document_id,source_hash,page_number) DO UPDATE SET`+
		` active_seconds=study_reading_spans.active_seconds+excluded.active_seconds,`+
		`visible_seconds=study_reading_spans.visible_seconds+excluded.visible_seconds,`+
		`ended_at=excluded.ended_at`,
		params.SessionID, params.CourseID, params.Document, params.SourceHash, params.Page,
		params.Action, params.Unit, params.Revision, params.Active, params.Visible)
	return err
}

func (w postgresWorkspace) AddAttention(ctx context.Context, session, active, visible int64) error {
	_, err := w.db.Exec(ctx, `UPDATE app.study_workspace_sessions`+
		` SET active_seconds=active_seconds+$2,visible_seconds=visible_seconds+$3,`+
		`last_seen_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$1`,
		session, active, visible)
	return err
}

func (w postgresWorkspace) ReadingTotals(
	ctx context.Context,
	course int64,
	action, document string,
) ([]database.ReadingPage, error) {
	rows, err := w.db.Query(ctx, `SELECT document_id,page_number,`+
		`sum(active_seconds)::bigint,sum(visible_seconds)::bigint FROM app.study_reading_spans`+
		` WHERE course_id=$1 AND ($2='' OR action_id=$2) AND ($3='' OR document_id=$3)`+
		` GROUP BY document_id,page_number ORDER BY document_id,page_number`,
		course, action, document)
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

func (w postgresWorkspace) WorkspaceDocument(
	ctx context.Context, course int64, id string,
) (database.WorkspaceDocument, error) {
	var doc database.WorkspaceDocument
	err := w.db.QueryRow(ctx, `SELECT display_name,source_path,source_origin,source_hash,document_kind,`+
		`mime_type,coalesce(page_count,0),reading_minutes,language_hint FROM knowledge.documents d`+
		` WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready'`+
		` AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id`+
		` WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.deleted_at IS NULL AND o.sha256=d.source_hash)`,
		id, course).Scan(
		&doc.DisplayName, &doc.SourcePath, &doc.SourceOrigin, &doc.SourceHash, &doc.DocumentKind,
		&doc.MimeType, &doc.PageCount, &doc.ReadingMinutes, &doc.LanguageHint)
	return doc, err
}

func (w postgresWorkspace) DocumentPending(ctx context.Context, course int64, id string) (bool, error) {
	var pending bool
	err := w.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.documents`+
		` WHERE id=$1 AND course_id=$2 AND is_current=1 AND status IN('pending','running'))`,
		id, course).Scan(&pending)
	return pending, err
}

func (w postgresWorkspace) DocumentInsights(
	ctx context.Context,
	params database.DocumentInsightParams,
) (database.DocumentInsightCounts, error) {
	var counts database.DocumentInsightCounts
	err := w.db.QueryRow(ctx, `SELECT`+
		` (SELECT count(*) FROM knowledge.page_enrichments`+
		` WHERE document_id=$1 AND source_hash=$2 AND analysis_version=$3 AND requested_model=$4 AND status='ready'),`+
		` (SELECT count(*) FROM app.study_annotations`+
		` WHERE document_id=$1 AND course_id=$5 AND status<>'deleted'),`+
		` (SELECT count(*) FROM app.study_annotations`+
		` WHERE document_id=$1 AND course_id=$5 AND status<>'deleted'`+
		` AND (status='orphaned' OR source_hash<>$2))`,
		params.Document, params.SourceHash, params.AnalysisVersion, params.RequestedModel,
		params.CourseID).Scan(&counts.ReadyPages, &counts.Annotations, &counts.Orphaned)
	return counts, err
}

func (w postgresWorkspace) CloseSession(
	ctx context.Context,
	params database.CloseSessionParams,
) (database.WorkspaceSession, error) {
	return scanWorkspaceSession(w.db.QueryRow(ctx,
		`UPDATE app.study_workspace_sessions SET outcome=$2,note=$3,confidence=$4,study_event_id=$5,`+
			`ended_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`+
			` WHERE id=$1 RETURNING `+workspaceSessionColumnsPG,
		params.ID, params.Outcome, params.Note, params.Confidence, params.EventID))
}
