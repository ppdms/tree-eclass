package annotations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool *pgxpool.Pool }

var ErrIdempotencyConflict = errors.New("idempotency key belongs to another document")

const visibleCourse = `SELECT c.id FROM app.courses c WHERE c.id=$1 AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1))`

const annotationJSON = `(to_jsonb(a)-'rects_json'-'tags_json'-'idempotency_key') || jsonb_build_object('rects',a.rects_json::jsonb,'tags',a.tags_json::jsonb)`

func (s Service) RequireCourse(ctx context.Context, id int64) error {
	var found int64
	return s.Pool.QueryRow(ctx, visibleCourse, id).Scan(&found)
}

func (s Service) Get(ctx context.Context, id int64) (Annotation, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT `+annotationJSON+` FROM app.study_annotations a WHERE id=$1`, id).Scan(&raw)
	if err != nil {
		return Annotation{}, err
	}
	return decode(raw)
}

func resolve(ctx context.Context, tx pgx.Tx, course int64, document string) (string, error) {
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
	if errors.Is(err, pgx.ErrNoRows) {
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

func insert(ctx context.Context, tx pgx.Tx, c Create) (int64, error) {
	var chunk *string
	err := tx.QueryRow(ctx, `SELECT id FROM knowledge.chunks WHERE document_id=$1 AND locator_type='page' AND locator_start ~ '^[0-9]+$' AND coalesce(locator_end,locator_start) ~ '^[0-9]+$' AND locator_start::bigint <= $2 AND coalesce(locator_end,locator_start)::bigint >= $2 ORDER BY ordinal LIMIT 1`, c.DocumentID, c.PageNumber).
		Scan(&chunk)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
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
		`SELECT `+annotationJSON+` FROM app.study_annotations a WHERE course_id=$1 AND ($2='' OR document_id=$2) AND ($3='' OR action_id=$3) AND ($4 OR status!='deleted') ORDER BY document_id,page_number,id LIMIT 1000`,
		course,
		document,
		identity.Encode(action),
		deleted,
	)
	if err != nil {
		return result, err
	}
	if err := scanAnnotations(rows, &result.Annotations); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func scanAnnotations(rows pgx.Rows, annotations *[]Annotation) error {
	seen := map[string]map[int64]bool{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		item, err := decode(raw)
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
