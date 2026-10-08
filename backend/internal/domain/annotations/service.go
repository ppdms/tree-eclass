package annotations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Service struct{ Pool rdbms.Pool }

var ErrIdempotencyConflict = errors.New("idempotency key belongs to another document")

const visibleCourse = `SELECT c.id FROM app.courses c WHERE c.id=$1 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1))`

// annotationColumns selects every study_annotations column in table order;
// assembleAnnotation builds the decode() JSON blob from explicit columns.
// rects_json/tags_json embed verbatim (stored JSON); chunk_id/body pass
// through as stored (nullable TEXT decodes the way to_jsonb nulls did).
const annotationColumns = `a.id,a.course_id,a.document_id,a.source_hash,a.page_number,a.kind,a.origin,a.status,a.color,a.quote,a.prefix,a.suffix,a.char_start,a.char_end,a.rects_json,a.chunk_id,a.body,a.tags_json,a.action_id,a.unit_key,a.plan_revision,a.session_id,a.created_at,a.updated_at`

func assembleAnnotation(
	id, courseID, page int64,
	document, hash, kind, origin, status, color, quote, prefix, suffix string,
	start, end *int64,
	rects string,
	chunkID *string,
	body *string,
	tags string,
	action, unit, revision string,
	session *int64,
	created, updated string,
) []byte {
	raw := map[string]any{
		"id": id, "course_id": courseID, "document_id": document,
		"source_hash": hash, "page_number": page, "kind": kind,
		"origin": origin, "status": status, "color": color,
		"quote": quote, "prefix": prefix, "suffix": suffix,
		"action_id": action, "unit_key": unit, "plan_revision": revision,
		"created_at": created, "updated_at": updated,
	}
	if start != nil {
		raw["char_start"] = *start
	}
	if end != nil {
		raw["char_end"] = *end
	}
	if session != nil {
		raw["session_id"] = *session
	}
	if chunkID != nil {
		raw["chunk_id"] = *chunkID
	}
	if body != nil {
		raw["body"] = *body
	}
	raw["rects"] = json.RawMessage(orJSONArray(rects))
	raw["tags"] = json.RawMessage(orJSONArray(tags))
	out, err := json.Marshal(raw)
	if err != nil {
		return []byte("{}")
	}
	return out
}

// orJSONArray passes stored JSON through, defaulting blanks to [].
func orJSONArray(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "[]"
	}
	return raw
}

// scanAnnotation reads one explicit-column single row.
func scanAnnotation(row rdbms.Row) (Annotation, error) {
	return scanColumns(row)
}

func (s Service) RequireCourse(ctx context.Context, id int64) error {
	var found int64
	return s.Pool.QueryRow(ctx, visibleCourse, id).Scan(&found)
}

func resolve(ctx context.Context, tx rdbms.Tx, course int64, document string) (string, error) {
	var hash string
	err := tx.QueryRow(ctx, `SELECT source_hash FROM knowledge.documents WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready' FOR SHARE`, document, course).
		Scan(&hash)
	return hash, err
}

func (s Service) Create(ctx context.Context, c Create) (Annotation, error) {
	if err := c.Validate(); err != nil {
		return Annotation{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Annotation{}, err
	}
	defer tx.Rollback(ctx)
	// Serialize bookmark and idempotency decisions across concurrent requests.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('annotation:'||$1,0))`, c.DocumentID); err != nil {
		return Annotation{}, err
	}
	c.SourceHash, err = resolve(ctx, tx, c.CourseID, c.DocumentID)
	if err != nil {
		return Annotation{}, err
	}
	var id int64
	err = tx.QueryRow(ctx, `SELECT id FROM app.study_annotations WHERE (idempotency_key=$1) OR ($2='bookmark' AND course_id=$3 AND document_id=$4 AND page_number=$5 AND kind='bookmark' AND status!='deleted') ORDER BY id LIMIT 1`, c.IdempotencyKey, c.Kind, c.CourseID, c.DocumentID, c.PageNumber).
		Scan(&id)
	if errors.Is(err, rdbms.ErrNoRows) {
		id, err = insert(ctx, tx, c)
	}
	if err != nil {
		return Annotation{}, err
	}
	var owner int64
	var document string
	if err = tx.QueryRow(ctx, `SELECT course_id,document_id FROM app.study_annotations WHERE id=$1`, id).Scan(&owner, &document); err != nil {
		return Annotation{}, err
	}
	if owner != c.CourseID || document != c.DocumentID {
		return Annotation{}, ErrIdempotencyConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return Annotation{}, err
	}
	return s.Get(ctx, id)
}

func insert(ctx context.Context, tx rdbms.Tx, c Create) (int64, error) {
	var chunk *string
	err := tx.QueryRow(ctx, `SELECT id FROM knowledge.chunks WHERE document_id=$1 AND locator_type='page' AND locator_start GLOB '[0-9]*' AND locator_start NOT GLOB '*[^0-9]*' AND coalesce(locator_end,locator_start) GLOB '[0-9]*' AND coalesce(locator_end,locator_start) NOT GLOB '*[^0-9]*' AND CAST(locator_start AS INTEGER) <= $2 AND CAST(coalesce(locator_end,locator_start) AS INTEGER) >= $2 ORDER BY ordinal LIMIT 1`, c.DocumentID, c.PageNumber).
		Scan(&chunk)
	if err != nil && !errors.Is(err, rdbms.ErrNoRows) {
		return 0, err
	}
	rects, _ := json.Marshal(c.Rects)
	tags, _ := json.Marshal(c.Tags)
	var body *string
	if c.Body != nil {
		text := identity.Encode(*c.Body)
		body = &text
	}
	var id int64
	err = tx.QueryRow(
		ctx,
		`INSERT INTO app.study_annotations(course_id,document_id,source_hash,page_number,kind,origin,color,quote,prefix,suffix,char_start,char_end,rects_json,chunk_id,body,tags_json,action_id,unit_key,plan_revision,session_id,idempotency_key)
 VALUES($1,$2,$3,$4,$5,'learner',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
 ON CONFLICT(idempotency_key) WHERE idempotency_key IS NOT NULL DO UPDATE SET idempotency_key=excluded.idempotency_key RETURNING id`,
		c.CourseID,
		c.DocumentID,
		c.SourceHash,
		c.PageNumber,
		c.Kind,
		identity.Encode(c.Color),
		identity.Encode(c.Quote),
		identity.Encode(c.Prefix),
		identity.Encode(c.Suffix),
		c.CharStart,
		c.CharEnd,
		string(rects),
		chunk,
		body,
		identity.Encode(string(tags)),
		identity.Encode(c.ActionID),
		identity.Encode(c.UnitKey),
		identity.Encode(c.PlanRevision),
		c.SessionID,
		c.IdempotencyKey,
	).
		Scan(&id)
	return id, err
}

// Get returns one annotation by id.
func (s Service) Get(ctx context.Context, id int64) (Annotation, error) {
	row := s.Pool.QueryRow(ctx, `SELECT `+annotationColumns+` FROM app.study_annotations a WHERE id=$1`, id)
	return scanAnnotation(row)
}

// scanColumns scans the explicit annotation columns from any scanner.
func scanColumns(scanner interface {
	Scan(dest ...any) error
},
) (Annotation, error) {
	var id, courseID, page int64
	var document, hash, kind, origin, status, color string
	var quote, prefix, suffix, action, unit, revision string
	var start, end, session *int64
	var rects, tags string
	var chunk, body *string
	var created, updated string
	if err := scanner.Scan(
		&id, &courseID, &document, &hash, &page, &kind, &origin, &status,
		&color, &quote, &prefix, &suffix, &start, &end, &rects, &chunk,
		&body, &tags, &action, &unit, &revision, &session, &created, &updated,
	); err != nil {
		return Annotation{}, err
	}
	raw := assembleAnnotation(
		id, courseID, page, document, hash, kind, origin, status, color,
		quote, prefix, suffix, start, end, rects, chunk, body, tags,
		action, unit, revision, session, created, updated,
	)
	return decode(raw)
}

type Listing struct {
	Annotations   []Annotation `json:"annotations"`
	NewlyOrphaned int64        `json:"newly_orphaned"`
	SourceHash    string       `json:"source_hash,omitempty"`
}

func (s Service) List(ctx context.Context, course int64, document, action string, deleted bool) (Listing, error) {
	result := Listing{Annotations: []Annotation{}}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	if document != "" {
		result.SourceHash, err = resolve(ctx, tx, course, document)
		if err != nil {
			return result, err
		}
		tag, err := tx.Exec(
			ctx,
			`UPDATE app.study_annotations SET status='orphaned',updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE document_id=$1 AND source_hash!=$2 AND status='active'`,
			document,
			result.SourceHash,
		)
		if err != nil {
			return result, err
		}
		result.NewlyOrphaned = tag.RowsAffected()
		action = "" // Document reads intentionally include every action's marks.
	}
	rows, err := tx.Query(
		ctx,
		`SELECT `+annotationColumns+` FROM app.study_annotations a WHERE course_id=$1 AND ($2='' OR document_id=$2) AND ($3='' OR action_id=$3) AND ($4 OR status!='deleted') ORDER BY document_id,page_number,id LIMIT 1000`,
		course,
		document,
		identity.Encode(action),
		deleted,
	)
	if err != nil {
		return result, err
	}
	if err := scanRows(rows, &result.Annotations); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

// scanRows collects explicit-column rows with bookmark dedup.
func scanRows(rows rdbms.Rows, annotations *[]Annotation) error {
	seen := map[string]map[int64]bool{}
	for rows.Next() {
		item, err := scanAnnotationRow(rows)
		if err != nil {
			rows.Close()
			return err
		}
		if item.Kind == "bookmark" && item.Status != "deleted" {
			if seen[item.DocumentID] == nil {
				seen[item.DocumentID] = map[int64]bool{}
			}
			if seen[item.DocumentID][item.PageNumber] {
				continue
			}
			seen[item.DocumentID][item.PageNumber] = true
		}
		*annotations = append(*annotations, item)
	}
	return rows.Err()
}

// scanAnnotationRow reads one explicit-column multi-row result.
func scanAnnotationRow(rows rdbms.Rows) (Annotation, error) {
	return scanColumns(rows)
}

func (s Service) Update(ctx context.Context, id int64, u Update) (Annotation, error) {
	if err := u.Validate(); err != nil {
		return Annotation{}, err
	}
	var body, color, tags *string
	if u.Body != nil {
		value := identity.Encode(clip(strings.TrimSpace(*u.Body), 20000))
		body = &value
	}
	if u.Color != nil {
		value := identity.Encode(clip(*u.Color, 32))
		color = &value
	}
	if u.Tags != nil {
		encoded, _ := json.Marshal(normalizeTags(*u.Tags))
		value := identity.Encode(string(encoded))
		tags = &value
	}
	_, err := s.Pool.Exec(
		ctx,
		`UPDATE app.study_annotations SET body=CASE WHEN $2::text IS NULL THEN body ELSE nullif($2,'') END,color=coalesce($3,color),tags_json=coalesce($4,tags_json),status=coalesce($5,status),updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$1`,
		id,
		body,
		color,
		tags,
		u.Status,
	)
	if err != nil {
		return Annotation{}, err
	}
	return s.Get(ctx, id)
}
