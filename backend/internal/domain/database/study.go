package database

import (
	"context"
	"time"
)

// StudyInboxItem is one unmastered file row for the learner inbox. Path, Name,
// URL, Redirect, CourseName and Prefix stay encoded exactly as stored; callers
// decode via identity.Decode. Updated is the stored upstream timestamp text,
// nil when never stamped. Level is the stored study level (0 when untracked);
// Priority is the native-computed urgency score.
type StudyInboxItem struct {
	Path       string  `json:"file_path"`
	Name       string  `json:"file_name"`
	URL        string  `json:"url"`
	Redirect   *string `json:"redirect_url"`
	Updated    *string `json:"last_updated"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Prefix     string  `json:"storage_prefix"`
	Level      int64   `json:"level"`
	Priority   float64 `json:"priority"`
}

// StudyPriorityMaterial is one ready document candidate for the intelligence
// projection. CourseName, Name and Path stay encoded exactly as stored;
// callers decode via identity.Decode. Reading, Pages and Words are nil when
// unset. Enrichment is the capped ready payload (nil when the document has
// no live ready enrichment for the requested generation).
type StudyPriorityMaterial struct {
	ID         string  `json:"id"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Name       string  `json:"display_name"`
	Path       string  `json:"source_path"`
	Origin     string  `json:"source_origin"`
	Kind       string  `json:"document_kind"`
	Complexity int64   `json:"complexity_score"`
	Reading    *int64  `json:"reading_minutes"`
	Pages      *int64  `json:"page_count"`
	Words      *int64  `json:"word_count"`
	Level      int64   `json:"level"`
	Enrichment *string `json:"enrichment_payload"`
}

// StudyIntelligenceParams scopes the priority-candidate read. Model is the
// configured enrichment model selecting live AI hints; DocumentVersion and
// PageVersion are the expected analysis generations (page generation applies
// to pdf/image documents, document generation to every other kind). Included
// nil reads every visible course; non-nil (even empty) restricts to the
// listed course ids.
type StudyIntelligenceParams struct {
	Model           string  `json:"model"`
	DocumentVersion string  `json:"document_version"`
	PageVersion     string  `json:"page_version"`
	Included        []int64 `json:"included"`
}

// StudySourceRow is one per-course generation row hashed into the projection
// fingerprint. Source, Config and Content are nil when the course has no
// published navigation row.
type StudySourceRow struct {
	Course    int64   `json:"course_id"`
	CourseGen int64   `json:"course_generation"`
	Learner   int64   `json:"learner_generation"`
	Source    *int64  `json:"source_generation"`
	Config    *string `json:"config_generation"`
	Content   *string `json:"content_id"`
}

// StudyMetric is one published projection row. Payload is the stored
// projection JSON; GeneratedAt is the stored generation timestamp text;
// SourceFingerprint pins the source snapshot; Status is ready or failed.
type StudyMetric struct {
	Payload           string `json:"payload_json"`
	SourceFingerprint string `json:"source_fingerprint"`
	Status            string `json:"status"`
	GeneratedAt       string `json:"generated_at"`
	Generation        int64  `json:"generation"`
}

// StudyScope is the next projection scope needing a build with the stored
// generation the publish must guard on (0 when no row exists yet).
type StudyScope struct {
	Scope      string `json:"scope"`
	Generation int64  `json:"generation"`
}

// StudyKnowledgeMeta aggregates admitted-document readiness for one scope.
type StudyKnowledgeMeta struct {
	Pending   int64   `json:"pending_documents"`
	Failed    int64   `json:"failed_documents"`
	Freshness *string `json:"freshness"`
}

// StudyEventRow is one stored learner event. Action, Revision and Unit stay
// encoded exactly as stored; callers decode via identity.Decode. Note stays
// stored-encoded (nil/empty when unset); Confidence and Minutes are nil when
// unset.
type StudyEventRow struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	Action     string  `json:"action_id"`
	Revision   string  `json:"plan_revision"`
	Unit       string  `json:"unit_key"`
	Type       string  `json:"event_type"`
	Confidence *int64  `json:"confidence"`
	Minutes    *int64  `json:"actual_minutes"`
	Note       *string `json:"note"`
}

// StudyEventParams records one learner event. Action, Revision, Unit and Note
// are identity-encoded before the call; Confidence and Minutes are nil when
// unset. Key carries the caller idempotency key verbatim.
type StudyEventParams struct {
	CourseID   int64   `json:"course_id"`
	Action     string  `json:"action_id"`
	Revision   string  `json:"plan_revision"`
	Unit       string  `json:"unit_key"`
	Type       string  `json:"event_type"`
	Key        string  `json:"idempotency_key"`
	Confidence *int64  `json:"confidence"`
	Minutes    *int64  `json:"actual_minutes"`
	Note       *string `json:"note"`
}

// StudyPublishParams writes one projection row. Payload is the projection
// JSON; GeneratedAt stamps the row; SourceFingerprint pins the built source
// snapshot; Status is ready or failed; Generation is the stored generation
// the write guards on (0 inserts when no row exists yet).
type StudyPublishParams struct {
	Scope             string    `json:"scope"`
	Payload           string    `json:"payload_json"`
	GeneratedAt       time.Time `json:"generated_at"`
	SourceFingerprint string    `json:"source_fingerprint"`
	Status            string    `json:"status"`
	Generation        int64     `json:"generation"`
}

// Study is the typed port for learner snapshots, study projections and
// learner events. Every method on a Tx binds to that transaction; read-only
// transactions reject mutations. Ordering, null, not-found and publication
// semantics are identical on both backends.
type Study interface {
	// ListInbox returns up to 60 unmastered files ordered by native
	// priority descending, then course id, then file id. Selected nil
	// reads visible courses only; non-nil also admits a hidden
	// committed course. Now is the reference instant for recency.
	ListInbox(ctx context.Context, selected *int64, now time.Time) ([]StudyInboxItem, error)
	// ListPriorityMaterials streams ready candidates ordered by course
	// id then normalized path. Both backends enforce the same source
	// admission (current ready verified content with text or pages and
	// a live catalog revision) and the same 4 MiB enrichment cap.
	ListPriorityMaterials(ctx context.Context, params StudyIntelligenceParams) (Iterator[StudyPriorityMaterial], error)
	// ListSourceRows returns per-course generation rows ordered by
	// course id for visible or committed courses.
	ListSourceRows(ctx context.Context) ([]StudySourceRow, error)
	// StudyMetric returns the stored projection row for one scope. It
	// reports ErrNoRows when no row exists or the payload exceeds the
	// 16 MiB publication budget.
	StudyMetric(ctx context.Context, scope string) (StudyMetric, error)
	// NextStaleScope returns the oldest scope whose stored fingerprint
	// differs or whose failed build is past retry, ordered by stored
	// generation time (missing rows first) then scope. It reports
	// ErrNoRows when every scope is current.
	NextStaleScope(ctx context.Context, fingerprint string) (StudyScope, error)
	// KnowledgeMeta aggregates admitted-document readiness for one
	// scope. Selected nil aggregates every visible course.
	KnowledgeMeta(ctx context.Context, selected *int64) (StudyKnowledgeMeta, error)
	// LockCourseForEvent locks the study-visible course row for an
	// event write. It reports ErrNoRows when the course is missing,
	// hidden and uncommitted, or meanwhile changed visibility.
	LockCourseForEvent(ctx context.Context, courseID int64) error
	// LockEventKey serializes same-key event writers inside the
	// transaction (extended advisory namespace). It requires a
	// transaction-bound handle.
	LockEventKey(ctx context.Context, key string) error
	// StudyEventByKey returns the stored event for one idempotency key.
	// It reports ErrNoRows when the key was never used.
	StudyEventByKey(ctx context.Context, key string) (StudyEventRow, error)
	// InsertStudyEvent appends one learner event and returns its id.
	InsertStudyEvent(ctx context.Context, params StudyEventParams) (int64, error)
	// PublishMetric writes one projection row guarded on the stored
	// generation. It reports whether the row was written; a lost
	// generation race or an existing newer row reports false with no
	// error.
	PublishMetric(ctx context.Context, params StudyPublishParams) (bool, error)
}
