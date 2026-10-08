package rdbms

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"
)

type sqliteStudy struct{ db nativeDBTX }

// studyStalenessSQLite renders the inbox recency factor natively: a stored
// last_updated parses only when it matches a full calendar-date shape
// (YYYY-MM-DD with a valid month/day); anything else (including upstream
// garbage like 'invalid-upstream-date') falls back to 30 days, exactly like
// the PostgreSQL pg_input_is_valid probe. Clamp/floor semantics mirror the
// PostgreSQL min/max/coalesce chain.
const studyStalenessSQLite = `min(90,max(0,coalesce(CASE WHEN
 f.last_updated GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
 AND substr(f.last_updated,6,2) BETWEEN '01' AND '12'
 AND substr(f.last_updated,9,2) BETWEEN '01' AND '31'
 AND datetime(f.last_updated) IS NOT NULL
 THEN (julianday(?)-julianday(f.last_updated)) END,30)))`

const studyInboxSQLite = `WITH listed AS (
 SELECT f.id,f.local_path,f.name,f.url,f.redirect_url,f.last_updated,
 n.course_id,c.name course_name,c.webdav_folder,coalesce(s.level,0) level
 FROM files f JOIN nodes n ON n.id=f.node_id
 JOIN courses c ON c.id=n.course_id
 LEFT JOIN file_study s
 ON s.course_id=n.course_id AND s.file_path=f.local_path
 WHERE f.local_path IS NOT NULL AND (? IS NULL OR c.id=?)
 AND (c.hidden=0 OR (? IS NOT NULL AND EXISTS(
 SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1)))
), completion AS (
 SELECT course_id,coalesce(sum(min(4,max(0,level)))
 FILTER(WHERE level<5)*1.0
 / nullif(4*count(*) FILTER(WHERE level<5),0),0) ratio
 FROM listed GROUP BY course_id
)
SELECT f.local_path,f.name,f.url,f.redirect_url,f.last_updated,
 f.course_id,f.course_name,f.webdav_folder,f.level,
 ` + studyStalenessSQLite + `*(1-c.ratio) priority
 FROM listed f JOIN completion c USING(course_id) WHERE f.level<4
 ORDER BY priority DESC,f.course_id,f.id LIMIT 60`

func (s sqliteStudy) ListInbox(
	ctx context.Context,
	selected *int64,
	now time.Time,
) ([]database.StudyInboxItem, error) {
	out := []database.StudyInboxItem{}
	stamp := now.UTC().Format(time.RFC3339Nano)
	rows, err := s.db.Query(
		ctx,
		studyInboxSQLite,
		selected,
		selected,
		selected,
		stamp,
	)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanStudyInbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

const studyMaterialsSQLite = `SELECT d.id,d.course_id,
 coalesce(nullif(c.short_name,''),c.name),
 d.display_name,d.source_path,d.source_origin,d.document_kind,
 coalesce(d.complexity_score,0),
 d.reading_minutes,d.page_count,d.word_count,coalesce(l.level,0),
 CASE WHEN e.status='ready'
 AND length(CAST(e.payload_json AS BLOB))<=4194304
 THEN e.payload_json END
 FROM documents d JOIN courses c ON c.id=d.course_id
 LEFT JOIN file_study l
 ON l.course_id=d.course_id AND l.file_path=d.source_path
 LEFT JOIN document_enrichments e
 ON e.document_id=d.id AND e.source_hash=d.source_hash
 AND coalesce(e.requested_model,e.model)=?
 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image')
 THEN ? ELSE ? END
 WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1))`

const studyMaterialsTailSQLite = `
 AND d.is_current=1 AND d.status='ready'
 AND d.content_hash_verified=1
 AND ((d.document_kind IN('pdf','image') AND coalesce(d.page_count,0)>0)
 OR EXISTS(SELECT 1 FROM chunks chunk
 WHERE chunk.document_id=d.id AND length(trim(chunk.text))>0))
 AND EXISTS(SELECT 1 FROM document_revisions r
 JOIN objects o ON o.id=r.object_id
 WHERE r.document_id=d.id AND r.course_id=d.course_id
 AND r.deleted_at IS NULL AND r.logical_path=d.normalized_path
 AND o.sha256=d.source_hash)
 ORDER BY d.course_id,d.normalized_path`

func (s sqliteStudy) ListPriorityMaterials(
	ctx context.Context,
	params database.StudyIntelligenceParams,
) (database.Iterator[database.StudyPriorityMaterial], error) {
	args := []any{params.Model, params.PageVersion, params.DocumentVersion}
	filter := ""
	if params.Included != nil {
		filter = " AND c.id IN " + sqlitePlaceholders(len(params.Included))
		for _, id := range params.Included {
			args = append(args, id)
		}
	}
	rows, err := s.db.Query(
		ctx,
		studyMaterialsSQLite+filter+studyMaterialsTailSQLite,
		args...,
	)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanStudyMaterial), nil
}

const studySourcesSQLite = `SELECT c.id,g.generation,
 coalesce(l.generation,0),
 n.source_generation,n.config_generation,n.content_id
 FROM courses c JOIN course_generation g ON g.course_id=c.id
 LEFT JOIN learner_generation l ON l.course_id=c.id
 LEFT JOIN navigation n ON n.course_id=c.id
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1) ORDER BY c.id`

func (s sqliteStudy) ListSourceRows(
	ctx context.Context,
) ([]database.StudySourceRow, error) {
	out := []database.StudySourceRow{}
	rows, err := s.db.Query(ctx, studySourcesSQLite)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row database.StudySourceRow
		err := rows.Scan(
			&row.Course,
			&row.CourseGen,
			&row.Learner,
			&row.Source,
			&row.Config,
			&row.Content,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s sqliteStudy) StudyMetric(
	ctx context.Context,
	scope string,
) (database.StudyMetric, error) {
	var row database.StudyMetric
	err := s.db.QueryRow(ctx, `SELECT payload_json,source_fingerprint,
 status,generated_at,generation FROM study_metrics
 WHERE scope=? AND length(CAST(payload_json AS BLOB))<=16777216`,
		scope,
	).Scan(
		&row.Payload,
		&row.SourceFingerprint,
		&row.Status,
		&row.GeneratedAt,
		&row.Generation,
	)
	return row, err
}

func (s sqliteStudy) NextStaleScope(
	ctx context.Context,
	fingerprint string,
) (database.StudyScope, error) {
	var row database.StudyScope
	// SQLite orders TEXT timestamps lexicographically; the schema writes
	// zero-padded UTC shapes so ordering matches timestamptz order.
	// Missing rows sort before any timestamp, like NULLS FIRST.
	err := s.db.QueryRow(ctx, `WITH scopes AS (
 SELECT 'all' scope UNION ALL SELECT 'course:'||c.id FROM courses c
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1)
) SELECT s.scope,coalesce(m.generation,0)
 FROM scopes s LEFT JOIN study_metrics m USING(scope)
 WHERE m.scope IS NULL OR m.source_fingerprint<>?
 OR (m.status='failed'
 AND m.retry_after<=strftime('%Y-%m-%d %H:%M:%S','now'))
 ORDER BY CASE WHEN m.generated_at IS NULL THEN 0 ELSE 1 END,
 m.generated_at,s.scope LIMIT 1`, fingerprint).
		Scan(&row.Scope, &row.Generation)
	return row, err
}

func (s sqliteStudy) KnowledgeMeta(
	ctx context.Context,
	selected *int64,
) (database.StudyKnowledgeMeta, error) {
	var row database.StudyKnowledgeMeta
	err := s.db.QueryRow(ctx, `SELECT
 count(*) FILTER(WHERE d.status IN('pending','running')),
 count(*) FILTER(WHERE d.status='failed'),max(d.indexed_at)
 FROM documents d JOIN courses c ON c.id=d.course_id
 WHERE d.is_current=1
 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1))
 AND (? IS NULL OR d.course_id=?)`, selected, selected).
		Scan(&row.Pending, &row.Failed, &row.Freshness)
	return row, err
}

func (s sqliteStudy) LockCourseForEvent(
	ctx context.Context,
	courseID int64,
) error {
	// No row locks on SQLite: the admitted writer owns every write until
	// commit/rollback, which is stronger than FOR SHARE. The guard still
	// verifies study visibility in the same snapshot as the event write.
	var id int64
	return s.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?
 AND (hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
 WHERE p.course_id=courses.id AND p.enabled=1))`,
		courseID,
	).Scan(&id)
}

func (s sqliteStudy) LockEventKey(
	ctx context.Context,
	key string,
) error {
	// The admitted writer owns every write until commit/rollback;
	// requires a writer transaction. The key is accepted for parity.
	_, err := advisoryLock(ctx, s.db, "study-event:"+key, true, false)
	return err
}

func (s sqliteStudy) StudyEventByKey(
	ctx context.Context,
	key string,
) (database.StudyEventRow, error) {
	var row database.StudyEventRow
	err := s.db.QueryRow(ctx, `SELECT id,course_id,action_id,
 plan_revision,unit_key,event_type,confidence,actual_minutes,note
 FROM study_unit_events WHERE idempotency_key=?`, key).
		Scan(
			&row.ID,
			&row.CourseID,
			&row.Action,
			&row.Revision,
			&row.Unit,
			&row.Type,
			&row.Confidence,
			&row.Minutes,
			&row.Note,
		)
	return row, err
}

func (s sqliteStudy) InsertStudyEvent(
	ctx context.Context,
	params database.StudyEventParams,
) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO study_unit_events(
 course_id,action_id,plan_revision,unit_key,event_type,
 idempotency_key,confidence,actual_minutes,note)
 VALUES(?,?,?,?,?,?,?,?,?) RETURNING id`,
		params.CourseID,
		params.Action,
		params.Revision,
		params.Unit,
		params.Type,
		params.Key,
		params.Confidence,
		params.Minutes,
		params.Note,
	).Scan(&id)
	return id, err
}

func (s sqliteStudy) PublishMetric(
	ctx context.Context,
	params database.StudyPublishParams,
) (bool, error) {
	stamp := params.GeneratedAt.UTC().Format(time.RFC3339Nano)
	result, err := s.db.Exec(ctx, `UPDATE study_metrics
 SET payload_json=?,generated_at=?,generation=generation+1,
 source_fingerprint=?,status=?,
 retry_after=CASE WHEN ?='failed'
 THEN datetime('now','+30 seconds') END
 WHERE scope=? AND generation=?`,
		params.Payload,
		stamp,
		params.SourceFingerprint,
		params.Status,
		params.Status,
		params.Scope,
		params.Generation,
	)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	// Insert the absent row. DO NOTHING reports a lost race as
	// not-published, never an error.
	result, err = s.db.Exec(ctx, `INSERT INTO study_metrics(
 scope,payload_json,generated_at,generation,
 source_fingerprint,status,retry_after)
 VALUES(?,?,?,1,?,?,
 CASE WHEN ?='failed' THEN datetime('now','+30 seconds') END)
 ON CONFLICT(scope) DO NOTHING`,
		params.Scope,
		params.Payload,
		stamp,
		params.SourceFingerprint,
		params.Status,
		params.Status,
	)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}
