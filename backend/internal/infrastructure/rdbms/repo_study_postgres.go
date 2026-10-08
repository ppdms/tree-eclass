package rdbms

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"
)

type postgresStudy struct{ db nativeDBTX }

func scanStudyInbox(rows nativeRows) (database.StudyInboxItem, error) {
	var item database.StudyInboxItem
	err := rows.Scan(
		&item.Path,
		&item.Name,
		&item.URL,
		&item.Redirect,
		&item.Updated,
		&item.CourseID,
		&item.CourseName,
		&item.Prefix,
		&item.Level,
		&item.Priority,
	)
	return item, err
}

const studyInboxPG = `WITH listed AS (
 SELECT f.id,f.local_path,f.name,f.url,f.redirect_url,f.last_updated,
 n.course_id,c.name course_name,c.webdav_folder,coalesce(s.level,0) level
 FROM app.files f JOIN app.nodes n ON n.id=f.node_id
 JOIN app.courses c ON c.id=n.course_id
 LEFT JOIN app.file_study s
 ON s.course_id=n.course_id AND s.file_path=f.local_path
 WHERE f.local_path IS NOT NULL AND ($1::bigint IS NULL OR c.id=$1)
 AND (c.hidden=0 OR ($1::bigint IS NOT NULL AND EXISTS(
 SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1)))
), completion AS (
 SELECT course_id,coalesce(sum(least(4,greatest(0,level)))
 FILTER(WHERE level<5)::double precision
 / nullif(4*count(*) FILTER(WHERE level<5),0),0) ratio
 FROM listed GROUP BY course_id
)
SELECT f.local_path,f.name,f.url,f.redirect_url,f.last_updated,
 f.course_id,f.course_name,f.webdav_folder,f.level,
 least(90,greatest(0,coalesce(CASE WHEN
 pg_input_is_valid(f.last_updated,'timestamptz')
 THEN extract(epoch FROM
 ($2::timestamptz-f.last_updated::timestamptz))/86400 END,30)))
 ::double precision*(1-c.ratio) priority
 FROM listed f JOIN completion c USING(course_id) WHERE f.level<4
 ORDER BY priority DESC,f.course_id,f.id LIMIT 60`

func (s postgresStudy) ListInbox(
	ctx context.Context,
	selected *int64,
	now time.Time,
) ([]database.StudyInboxItem, error) {
	out := []database.StudyInboxItem{}
	rows, err := s.db.Query(ctx, studyInboxPG, selected, now)
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

func scanStudyMaterial(
	rows nativeRows,
) (database.StudyPriorityMaterial, error) {
	var row database.StudyPriorityMaterial
	err := rows.Scan(
		&row.ID,
		&row.CourseID,
		&row.CourseName,
		&row.Name,
		&row.Path,
		&row.Origin,
		&row.Kind,
		&row.Complexity,
		&row.Reading,
		&row.Pages,
		&row.Words,
		&row.Level,
		&row.Enrichment,
	)
	return row, err
}

const studyMaterialsPG = `SELECT d.id,d.course_id,
 coalesce(nullif(c.short_name,''),c.name),
 d.display_name,d.source_path,d.source_origin,d.document_kind,
 coalesce(d.complexity_score,0),
 d.reading_minutes,d.page_count,d.word_count,coalesce(l.level,0),
 CASE WHEN e.status='ready'
 AND octet_length(e.payload_json)<=4194304 THEN e.payload_json END
 FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id
 LEFT JOIN app.file_study l
 ON l.course_id=d.course_id AND l.file_path=d.source_path
 LEFT JOIN knowledge.document_enrichments e
 ON e.document_id=d.id AND e.source_hash=d.source_hash
 AND coalesce(e.requested_model,e.model)=$1
 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image')
 THEN $3 ELSE $2 END
 WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1))`

const studyMaterialsTailPG = `
 AND d.is_current=1 AND d.status='ready'
 AND d.content_hash_verified=1
 AND ((d.document_kind IN('pdf','image') AND coalesce(d.page_count,0)>0)
 OR EXISTS(SELECT 1 FROM knowledge.chunks chunk
 WHERE chunk.document_id=d.id AND length(trim(chunk.text))>0))
 AND EXISTS(SELECT 1 FROM app.document_revisions r
 JOIN app.objects o ON o.id=r.object_id
 WHERE r.document_id=d.id AND r.course_id=d.course_id
 AND r.deleted_at IS NULL AND r.logical_path=d.normalized_path
 AND o.sha256=d.source_hash)
 ORDER BY d.course_id,d.normalized_path`

func (s postgresStudy) ListPriorityMaterials(
	ctx context.Context,
	params database.StudyIntelligenceParams,
) (database.Iterator[database.StudyPriorityMaterial], error) {
	args := []any{params.Model, params.DocumentVersion, params.PageVersion}
	filter := ""
	if params.Included != nil {
		filter = " AND c.id IN " + pgPlaceholders(4, len(params.Included))
		for _, id := range params.Included {
			args = append(args, id)
		}
	}
	rows, err := s.db.Query(
		ctx,
		studyMaterialsPG+filter+studyMaterialsTailPG,
		args...,
	)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanStudyMaterial), nil
}

const studySourcesPG = `SELECT c.id,g.generation,coalesce(l.generation,0),
 n.source_generation,n.config_generation,n.content_id
 FROM app.courses c JOIN read_model.course_generation g
 ON g.course_id=c.id
 LEFT JOIN read_model.learner_generation l ON l.course_id=c.id
 LEFT JOIN read_model.navigation n ON n.course_id=c.id
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1) ORDER BY c.id`

func (s postgresStudy) ListSourceRows(
	ctx context.Context,
) ([]database.StudySourceRow, error) {
	out := []database.StudySourceRow{}
	rows, err := s.db.Query(ctx, studySourcesPG)
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

func (s postgresStudy) StudyMetric(
	ctx context.Context,
	scope string,
) (database.StudyMetric, error) {
	var row database.StudyMetric
	err := s.db.QueryRow(ctx, `SELECT payload_json,source_fingerprint,
 status,generated_at,generation FROM read_model.study_metrics
 WHERE scope=$1 AND octet_length(payload_json)<=16777216`, scope).
		Scan(
			&row.Payload,
			&row.SourceFingerprint,
			&row.Status,
			&row.GeneratedAt,
			&row.Generation,
		)
	return row, err
}

func (s postgresStudy) NextStaleScope(
	ctx context.Context,
	fingerprint string,
) (database.StudyScope, error) {
	var row database.StudyScope
	err := s.db.QueryRow(ctx, `WITH scopes AS (
 SELECT 'all' scope UNION ALL SELECT 'course:'||c.id
 FROM app.courses c
 WHERE c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1)
) SELECT s.scope,coalesce(m.generation,0)
 FROM scopes s LEFT JOIN read_model.study_metrics m USING(scope)
 WHERE m.scope IS NULL OR m.source_fingerprint<>$1
 OR (m.status='failed' AND m.retry_after<=now())
 ORDER BY m.generated_at NULLS FIRST,s.scope LIMIT 1`,
		fingerprint,
	).Scan(&row.Scope, &row.Generation)
	return row, err
}

func (s postgresStudy) KnowledgeMeta(
	ctx context.Context,
	selected *int64,
) (database.StudyKnowledgeMeta, error) {
	var row database.StudyKnowledgeMeta
	err := s.db.QueryRow(ctx, `SELECT
 count(*) FILTER(WHERE d.status IN('pending','running')),
 count(*) FILTER(WHERE d.status='failed'),max(d.indexed_at)
 FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id
 WHERE d.is_current=1
 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=c.id AND p.enabled=1))
 AND ($1::bigint IS NULL OR d.course_id=$1)`, selected).
		Scan(&row.Pending, &row.Failed, &row.Freshness)
	return row, err
}

func (s postgresStudy) LockCourseForEvent(
	ctx context.Context,
	courseID int64,
) error {
	var id int64
	return s.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1
 AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
 WHERE p.course_id=app.courses.id AND p.enabled=1)) FOR SHARE`,
		courseID,
	).Scan(&id)
}

func (s postgresStudy) LockEventKey(
	ctx context.Context,
	key string,
) error {
	_, err := advisoryLock(ctx, s.db, "study-event:"+key, true, false)
	return err
}

func (s postgresStudy) StudyEventByKey(
	ctx context.Context,
	key string,
) (database.StudyEventRow, error) {
	var row database.StudyEventRow
	err := s.db.QueryRow(ctx, `SELECT id,course_id,action_id,
 plan_revision,unit_key,event_type,confidence,actual_minutes,note
 FROM app.study_unit_events WHERE idempotency_key=$1`, key).
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

func (s postgresStudy) InsertStudyEvent(
	ctx context.Context,
	params database.StudyEventParams,
) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO app.study_unit_events(
 course_id,action_id,plan_revision,unit_key,event_type,
 idempotency_key,confidence,actual_minutes,note)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
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

func (s postgresStudy) PublishMetric(
	ctx context.Context,
	params database.StudyPublishParams,
) (bool, error) {
	stamp := params.GeneratedAt.UTC().Format(time.RFC3339Nano)
	result, err := s.db.Exec(ctx, `UPDATE read_model.study_metrics
 SET payload_json=$2,generated_at=$3,generation=generation+1,
 source_fingerprint=$4,status=$5,
 retry_after=CASE WHEN $5='failed'
 THEN now()+interval '30 seconds' END
 WHERE scope=$1 AND generation=$6`,
		params.Scope,
		params.Payload,
		stamp,
		params.SourceFingerprint,
		params.Status,
		params.Generation,
	)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	// Insert the absent row. ON CONFLICT DO NOTHING reports a lost race
	// (or a concurrent insert winning first) as not-published, never an
	// error; a duplicate payload write is always a no-op.
	result, err = s.db.Exec(ctx, `INSERT INTO read_model.study_metrics(
 scope,payload_json,generated_at,generation,
 source_fingerprint,status,retry_after)
 VALUES($1,$2,$3,1,$4,$5,
 CASE WHEN $5='failed' THEN now()+interval '30 seconds' END)
 ON CONFLICT(scope) DO NOTHING`,
		params.Scope,
		params.Payload,
		stamp,
		params.SourceFingerprint,
		params.Status,
	)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}
