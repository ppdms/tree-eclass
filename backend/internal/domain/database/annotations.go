package database

import "context"

// AnnotationRow is one stored learner annotation. Quote, Prefix, Suffix,
// Color, Action, Unit, Revision and TagsJSON stay encoded exactly as stored;
// Body stays stored-encoded when non-nil. RectsJSON holds the stored rect
// array bytes verbatim. ChunkID and Session are nil when unset. Created and
// Updated are schema timestamp texts exactly as recorded.
type AnnotationRow struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	DocumentID string  `json:"document_id"`
	SourceHash string  `json:"source_hash"`
	PageNumber int64   `json:"page_number"`
	Kind       string  `json:"kind"`
	Origin     string  `json:"origin"`
	Status     string  `json:"status"`
	Color      string  `json:"color"`
	Quote      string  `json:"quote"`
	Prefix     string  `json:"prefix"`
	Suffix     string  `json:"suffix"`
	CharStart  *int64  `json:"char_start"`
	CharEnd    *int64  `json:"char_end"`
	RectsJSON  string  `json:"rects_json"`
	ChunkID    *string `json:"chunk_id"`
	Body       *string `json:"body"`
	TagsJSON   string  `json:"tags_json"`
	Action     string  `json:"action_id"`
	Unit       string  `json:"unit_key"`
	Revision   string  `json:"plan_revision"`
	Session    *int64  `json:"session_id"`
	Created    string  `json:"created_at"`
	Updated    string  `json:"updated_at"`
}

// AnnotationSnapshotRow pairs one stored annotation with the joined current
// source hash for its document. Current is nil when the document has no
// current row (the mark's source row is gone).
type AnnotationSnapshotRow struct {
	AnnotationRow
	Current *string `json:"current_source_hash"`
}

// ListAnnotationsParams scopes mark listing. Action is already
// identity-encoded by the caller; empty Document or Action disables that
// filter. Deleted includes soft-deleted rows.
type ListAnnotationsParams struct {
	CourseID int64  `json:"course_id"`
	Document string `json:"document_id"`
	Action   string `json:"action_id"`
	Deleted  bool   `json:"include_deleted"`
}

// InsertAnnotationParams journals one learner mark. SourceHash is the resolved
// current ready hash; Color, Quote, Prefix, Suffix, Action, Unit, Revision
// and TagsJSON are already identity-encoded by the caller; Body is already
// encoded when non-nil. RectsJSON holds the rect array bytes verbatim.
// ChunkID pins the covering page chunk when one matched, nil otherwise.
// Session is nil when the mark is not attached to a sitting. Key carries the
// caller idempotency key verbatim.
type InsertAnnotationParams struct {
	CourseID   int64   `json:"course_id"`
	DocumentID string  `json:"document_id"`
	SourceHash string  `json:"source_hash"`
	PageNumber int64   `json:"page_number"`
	Kind       string  `json:"kind"`
	Color      string  `json:"color"`
	Quote      string  `json:"quote"`
	Prefix     string  `json:"prefix"`
	Suffix     string  `json:"suffix"`
	CharStart  *int64  `json:"char_start"`
	CharEnd    *int64  `json:"char_end"`
	RectsJSON  string  `json:"rects_json"`
	ChunkID    *string `json:"chunk_id"`
	Body       *string `json:"body"`
	TagsJSON   string  `json:"tags_json"`
	Action     string  `json:"action_id"`
	Unit       string  `json:"unit_key"`
	Revision   string  `json:"plan_revision"`
	Session    *int64  `json:"session_id"`
	Key        string  `json:"idempotency_key"`
}

// UpdateAnnotationParams patches one learner mark. Body is already
// identity-encoded when non-nil (empty clears the note); Color and TagsJSON
// are already encoded when non-nil; Status is nil when unchanged.
type UpdateAnnotationParams struct {
	ID     int64   `json:"id"`
	Body   *string `json:"body"`
	Color  *string `json:"color"`
	Tags   *string `json:"tags_json"`
	Status *string `json:"status"`
}

// AnnotationOwner is the course and document scope of one stored mark.
type AnnotationOwner struct {
	CourseID int64  `json:"course_id"`
	Document string `json:"document_id"`
}

// Annotations is the typed port for learner evidence marks: idempotent
// creation, point reads, bounded listing with durable orphaning, snapshot
// reads that never rewrite anchors, and patches. Every method on a Tx binds
// to that transaction; read-only transactions reject mutations.
type Annotations interface {
	// CheckVisibleCourse reports ErrNoRows when the course is missing or
	// not study-visible, without taking row locks.
	CheckVisibleCourse(ctx context.Context, course int64) error
	// LockAnnotationKey serializes same-document idempotency and bookmark
	// decisions inside the transaction (extended advisory namespace). It
	// requires a transaction-bound handle.
	LockAnnotationKey(ctx context.Context, document string) error
	// ReadyDocumentHash returns the current ready hash for a course
	// document under a shared lock. It reports ErrNoRows when the
	// document is not current and ready.
	ReadyDocumentHash(ctx context.Context, course int64, document string) (string, error)
	// FindExisting returns the lowest stored mark id matching the
	// idempotency key or, for bookmarks, the live page bookmark. It
	// reports ErrNoRows when no stored row matches.
	FindExisting(ctx context.Context, key, kind string, course int64, document string, page int64) (int64, error)
	// PageChunkID returns the lowest-ordinal page chunk covering one page
	// whose locators are whole non-negative integers. It reports ErrNoRows
	// when no stored chunk covers the page.
	PageChunkID(ctx context.Context, document string, page int64) (string, error)
	// InsertAnnotation journals one learner mark and returns its id. A
	// concurrent same-key insert returns the winner's id instead of
	// failing. It runs on the caller's transaction.
	InsertAnnotation(ctx context.Context, params InsertAnnotationParams) (int64, error)
	// AnnotationOwner returns the stored course and document scope of one
	// mark. It reports ErrNoRows when the mark is missing.
	AnnotationOwner(ctx context.Context, id int64) (AnnotationOwner, error)
	// AnnotationByID returns one stored mark by id. It reports ErrNoRows
	// when the mark is missing.
	AnnotationByID(ctx context.Context, id int64) (AnnotationRow, error)
	// MarkOrphaned durably orphans active marks whose source revision no
	// longer matches and returns the count rewritten.
	MarkOrphaned(ctx context.Context, document, hash string) (int64, error)
	// ListAnnotations returns stored marks in (document, page, id) order,
	// capped at 1000 rows.
	ListAnnotations(ctx context.Context, params ListAnnotationsParams) ([]AnnotationRow, error)
	// SnapshotAnnotations returns stored marks joined to their document's
	// current source hash in (document, page, id) order, capped at 1001
	// rows. Callers enforce the bounded response budget over the result.
	SnapshotAnnotations(ctx context.Context, params ListAnnotationsParams) ([]AnnotationSnapshotRow, error)
	// UpdateAnnotation patches one mark and refreshes its timestamp.
	UpdateAnnotation(ctx context.Context, params UpdateAnnotationParams) error
}
