package annotations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool database.Store }

var ErrIdempotencyConflict = errors.New("idempotency key belongs to another document")

func annotationFromRow(row database.AnnotationRow) (Annotation, error) {
	raw := assembleAnnotation(
		row.ID, row.CourseID, row.PageNumber, row.DocumentID, row.SourceHash, row.Kind,
		row.Origin, row.Status, row.Color, row.Quote, row.Prefix, row.Suffix,
		row.CharStart, row.CharEnd, row.RectsJSON, row.ChunkID, row.Body, row.TagsJSON,
		row.Action, row.Unit, row.Revision, row.Session, row.Created, row.Updated,
	)
	return decode(raw)
}

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

func (s Service) RequireCourse(ctx context.Context, id int64) error {
	return s.Pool.Annotations().CheckVisibleCourse(ctx, id)
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
	if err = tx.Annotations().LockAnnotationKey(ctx, c.DocumentID); err != nil {
		return Annotation{}, err
	}
	c.SourceHash, err = tx.Annotations().ReadyDocumentHash(ctx, c.CourseID, c.DocumentID)
	if err != nil {
		return Annotation{}, err
	}
	id, err := tx.Annotations().FindExisting(ctx, c.IdempotencyKey, c.Kind, c.CourseID, c.DocumentID, c.PageNumber)
	if errors.Is(err, database.ErrNoRows) {
		id, err = insert(ctx, tx, c)
	}
	if err != nil {
		return Annotation{}, err
	}
	owner, err := tx.Annotations().AnnotationOwner(ctx, id)
	if err != nil {
		return Annotation{}, err
	}
	if owner.CourseID != c.CourseID || owner.Document != c.DocumentID {
		return Annotation{}, ErrIdempotencyConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return Annotation{}, err
	}
	return s.Get(ctx, id)
}

func insert(ctx context.Context, tx database.Tx, c Create) (int64, error) {
	chunk, err := tx.Annotations().PageChunkID(ctx, c.DocumentID, c.PageNumber)
	var chunkID *string
	if err == nil {
		chunkID = &chunk
	} else if !errors.Is(err, database.ErrNoRows) {
		return 0, err
	}
	rects, _ := json.Marshal(c.Rects)
	tags, _ := json.Marshal(c.Tags)
	var body *string
	if c.Body != nil {
		text := identity.Encode(*c.Body)
		body = &text
	}
	return tx.Annotations().InsertAnnotation(ctx, database.InsertAnnotationParams{
		CourseID:   c.CourseID,
		DocumentID: c.DocumentID,
		SourceHash: c.SourceHash,
		PageNumber: c.PageNumber,
		Kind:       c.Kind,
		Color:      identity.Encode(c.Color),
		Quote:      identity.Encode(c.Quote),
		Prefix:     identity.Encode(c.Prefix),
		Suffix:     identity.Encode(c.Suffix),
		CharStart:  c.CharStart,
		CharEnd:    c.CharEnd,
		RectsJSON:  string(rects),
		ChunkID:    chunkID,
		Body:       body,
		TagsJSON:   identity.Encode(string(tags)),
		Action:     identity.Encode(c.ActionID),
		Unit:       identity.Encode(c.UnitKey),
		Revision:   identity.Encode(c.PlanRevision),
		Session:    c.SessionID,
		Key:        c.IdempotencyKey,
	})
}

// Get returns one annotation by id.
func (s Service) Get(ctx context.Context, id int64) (Annotation, error) {
	row, err := s.Pool.Annotations().AnnotationByID(ctx, id)
	if err != nil {
		return Annotation{}, err
	}
	return annotationFromRow(row)
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
		result.SourceHash, err = tx.Annotations().ReadyDocumentHash(ctx, course, document)
		if err != nil {
			return result, err
		}
		result.NewlyOrphaned, err = tx.Annotations().MarkOrphaned(ctx, document, result.SourceHash)
		if err != nil {
			return result, err
		}
		action = "" // Document reads intentionally include every action's marks.
	}
	rows, err := tx.Annotations().ListAnnotations(ctx, database.ListAnnotationsParams{
		CourseID: course,
		Document: document,
		Action:   identity.Encode(action),
		Deleted:  deleted,
	})
	if err != nil {
		return result, err
	}
	if err := collectRows(rows, &result.Annotations); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

// collectRows decodes stored rows with bookmark dedup.
func collectRows(rows []database.AnnotationRow, annotations *[]Annotation) error {
	seen := map[string]map[int64]bool{}
	for _, row := range rows {
		item, err := annotationFromRow(row)
		if err != nil {
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
	return nil
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
	if err := s.Pool.Annotations().UpdateAnnotation(ctx, database.UpdateAnnotationParams{
		ID:     id,
		Body:   body,
		Color:  color,
		Tags:   tags,
		Status: u.Status,
	}); err != nil {
		return Annotation{}, err
	}
	return s.Get(ctx, id)
}
