package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresAnnotations struct{ db nativeDBTX }

const annotationColumnsPG = `id,course_id,document_id,source_hash,page_number,kind,origin,status,color,` +
	`quote,prefix,suffix,char_start,char_end,rects_json,chunk_id,body,tags_json,action_id,unit_key,` +
	`plan_revision,session_id,created_at,updated_at`

func scanAnnotationRowPG(row nativeRow) (database.AnnotationRow, error) {
	var item database.AnnotationRow
	err := row.Scan(
		&item.ID, &item.CourseID, &item.DocumentID, &item.SourceHash, &item.PageNumber,
		&item.Kind, &item.Origin, &item.Status, &item.Color, &item.Quote, &item.Prefix,
		&item.Suffix, &item.CharStart, &item.CharEnd, &item.RectsJSON, &item.ChunkID,
		&item.Body, &item.TagsJSON, &item.Action, &item.Unit, &item.Revision,
		&item.Session, &item.Created, &item.Updated,
	)
	return item, err
}

func scanAnnotationRowsPG(rows nativeRows, out *[]database.AnnotationRow) error {
	for rows.Next() {
		var item database.AnnotationRow
		if err := rows.Scan(
			&item.ID, &item.CourseID, &item.DocumentID, &item.SourceHash, &item.PageNumber,
			&item.Kind, &item.Origin, &item.Status, &item.Color, &item.Quote, &item.Prefix,
			&item.Suffix, &item.CharStart, &item.CharEnd, &item.RectsJSON, &item.ChunkID,
			&item.Body, &item.TagsJSON, &item.Action, &item.Unit, &item.Revision,
			&item.Session, &item.Created, &item.Updated,
		); err != nil {
			return err
		}
		*out = append(*out, item)
	}
	return rows.Err()
}

func (a postgresAnnotations) CheckVisibleCourse(ctx context.Context, course int64) error {
	var id int64
	return a.db.QueryRow(ctx, `SELECT c.id FROM app.courses c WHERE c.id=$1`+
		` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p`+
		` WHERE p.course_id=c.id AND p.enabled=1))`, course).Scan(&id)
}

func (a postgresAnnotations) LockAnnotationKey(ctx context.Context, document string) error {
	_, err := advisoryLock(ctx, a.db, "annotation:"+document, true, false)
	return err
}

func (a postgresAnnotations) ReadyDocumentHash(ctx context.Context, course int64, document string) (string, error) {
	var hash string
	err := a.db.QueryRow(ctx, `SELECT source_hash FROM knowledge.documents`+
		` WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready' FOR SHARE`,
		document, course).Scan(&hash)
	return hash, err
}

func (a postgresAnnotations) FindExisting(
	ctx context.Context,
	key, kind string,
	course int64,
	document string,
	page int64,
) (int64, error) {
	var id int64
	err := a.db.QueryRow(ctx, `SELECT id FROM app.study_annotations`+
		` WHERE (idempotency_key=$1)`+
		` OR ($2='bookmark' AND course_id=$3 AND document_id=$4 AND page_number=$5`+
		` AND kind='bookmark' AND status!='deleted') ORDER BY id LIMIT 1`,
		key, kind, course, document, page).Scan(&id)
	return id, err
}

func (a postgresAnnotations) PageChunkID(ctx context.Context, document string, page int64) (string, error) {
	// Native whole-integer locator match: one or more ASCII digits only.
	// NUMERIC casts never overflow on validated digits, so no stored
	// locator can throw; SQLite CAST clamps huge values with identical
	// comparison outcomes.
	var id string
	err := a.db.QueryRow(ctx, `SELECT id FROM knowledge.chunks WHERE document_id=$1 AND locator_type='page'`+
		` AND locator_start ~ '^[0-9]+$' AND coalesce(locator_end,locator_start) ~ '^[0-9]+$'`+
		` AND CAST(locator_start AS NUMERIC) <= $2`+
		` AND CAST(coalesce(locator_end,locator_start) AS NUMERIC) >= $2`+
		` ORDER BY ordinal LIMIT 1`, document, page).Scan(&id)
	return id, err
}

func (a postgresAnnotations) InsertAnnotation(
	ctx context.Context,
	params database.InsertAnnotationParams,
) (int64, error) {
	var id int64
	err := a.db.QueryRow(ctx, `INSERT INTO app.study_annotations(course_id,document_id,source_hash,`+
		`page_number,kind,origin,color,quote,prefix,suffix,char_start,char_end,rects_json,chunk_id,`+
		`body,tags_json,action_id,unit_key,plan_revision,session_id,idempotency_key)`+
		` VALUES($1,$2,$3,$4,$5,'learner',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`+
		` ON CONFLICT(idempotency_key) WHERE idempotency_key IS NOT NULL`+
		` DO UPDATE SET idempotency_key=excluded.idempotency_key RETURNING id`,
		params.CourseID, params.DocumentID, params.SourceHash, params.PageNumber, params.Kind,
		params.Color, params.Quote, params.Prefix, params.Suffix, params.CharStart, params.CharEnd,
		params.RectsJSON, params.ChunkID, params.Body, params.TagsJSON, params.Action,
		params.Unit, params.Revision, params.Session, params.Key).Scan(&id)
	return id, err
}

func (a postgresAnnotations) AnnotationOwner(ctx context.Context, id int64) (database.AnnotationOwner, error) {
	var owner database.AnnotationOwner
	err := a.db.QueryRow(ctx, `SELECT course_id,document_id FROM app.study_annotations WHERE id=$1`,
		id).Scan(&owner.CourseID, &owner.Document)
	return owner, err
}

func (a postgresAnnotations) AnnotationByID(ctx context.Context, id int64) (database.AnnotationRow, error) {
	return scanAnnotationRowPG(a.db.QueryRow(ctx,
		`SELECT `+annotationColumnsPG+` FROM app.study_annotations WHERE id=$1`, id))
}

func (a postgresAnnotations) MarkOrphaned(ctx context.Context, document, hash string) (int64, error) {
	result, err := a.db.Exec(ctx, `UPDATE app.study_annotations SET status='orphaned',`+
		`updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`+
		` WHERE document_id=$1 AND source_hash!=$2 AND status='active'`, document, hash)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (a postgresAnnotations) ListAnnotations(
	ctx context.Context,
	params database.ListAnnotationsParams,
) ([]database.AnnotationRow, error) {
	rows, err := a.db.Query(ctx, `SELECT `+annotationColumnsPG+` FROM app.study_annotations`+
		` WHERE course_id=$1 AND ($2='' OR document_id=$2) AND ($3='' OR action_id=$3)`+
		` AND ($4 OR status!='deleted') ORDER BY document_id,page_number,id LIMIT 1000`,
		params.CourseID, params.Document, params.Action, params.Deleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.AnnotationRow{}
	if err := scanAnnotationRowsPG(rows, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a postgresAnnotations) SnapshotAnnotations(
	ctx context.Context,
	params database.ListAnnotationsParams,
) ([]database.AnnotationSnapshotRow, error) {
	rows, err := a.db.Query(ctx, `SELECT a.id,a.course_id,a.document_id,a.source_hash,a.page_number,`+
		`a.kind,a.origin,a.status,a.color,a.quote,a.prefix,a.suffix,a.char_start,a.char_end,`+
		`a.rects_json,a.chunk_id,a.body,a.tags_json,a.action_id,a.unit_key,a.plan_revision,`+
		`a.session_id,a.created_at,a.updated_at,d.source_hash`+
		` FROM app.study_annotations a`+
		` LEFT JOIN knowledge.documents d ON d.id=a.document_id AND d.course_id=a.course_id AND d.is_current=1`+
		` WHERE a.course_id=$1 AND ($2='' OR a.document_id=$2) AND ($3='' OR a.action_id=$3)`+
		` AND ($4 OR a.status!='deleted') ORDER BY a.document_id,a.page_number,a.id LIMIT 1001`,
		params.CourseID, params.Document, params.Action, params.Deleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.AnnotationSnapshotRow{}
	for rows.Next() {
		var item database.AnnotationSnapshotRow
		row := &item.AnnotationRow
		if err := rows.Scan(
			&row.ID, &row.CourseID, &row.DocumentID, &row.SourceHash, &row.PageNumber,
			&row.Kind, &row.Origin, &row.Status, &row.Color, &row.Quote, &row.Prefix,
			&row.Suffix, &row.CharStart, &row.CharEnd, &row.RectsJSON, &row.ChunkID,
			&row.Body, &row.TagsJSON, &row.Action, &row.Unit, &row.Revision,
			&row.Session, &row.Created, &row.Updated,
			&item.Current,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (a postgresAnnotations) UpdateAnnotation(ctx context.Context, params database.UpdateAnnotationParams) error {
	_, err := a.db.Exec(ctx, `UPDATE app.study_annotations SET`+
		` body=CASE WHEN $2::text IS NULL THEN body ELSE nullif($2,'') END,`+
		`color=coalesce($3,color),tags_json=coalesce($4,tags_json),status=coalesce($5,status),`+
		`updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$1`,
		params.ID, params.Body, params.Color, params.Tags, params.Status)
	return err
}
