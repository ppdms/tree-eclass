package database

import "context"

// WorkspaceSession is the stored study-session row. Action, Unit, Revision
// and Note stay encoded exactly as stored; callers decode via identity.Decode.
// Started, Seen and Ended are schema timestamp texts exactly as recorded.
type WorkspaceSession struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	Action     string  `json:"action_id"`
	Unit       string  `json:"unit_key"`
	Revision   string  `json:"plan_revision"`
	Key        string  `json:"client_session_key"`
	Planned    *int64  `json:"planned_minutes"`
	Active     int64   `json:"active_seconds"`
	Visible    int64   `json:"visible_seconds"`
	Outcome    *string `json:"outcome"`
	Note       *string `json:"note"`
	Started    *string `json:"started_at"`
	Seen       *string `json:"last_seen_at"`
	Ended      *string `json:"ended_at"`
	Confidence *int64  `json:"confidence"`
	EventID    *int64  `json:"study_event_id"`
}

// InsertSessionParams journals one study sitting. Action, Unit and Revision
// are already identity-encoded by the caller; empty selects a document
// sitting with no planned action.
type InsertSessionParams struct {
	CourseID int64  `json:"course_id"`
	Action   string `json:"action_id"`
	Unit     string `json:"unit_key"`
	Revision string `json:"plan_revision"`
	Key      string `json:"client_session_key"`
	Planned  *int64 `json:"planned_minutes"`
}

// AccumulateSpanParams attributes one heartbeat interval to the exact source
// revision on one page. Action, Unit and Revision are already encoded.
// Active holds the active seconds (0 when the reader was idle) and Visible
// the visible seconds; both are already clamped by the caller.
type AccumulateSpanParams struct {
	SessionID  int64  `json:"session_id"`
	CourseID   int64  `json:"course_id"`
	Document   string `json:"document_id"`
	SourceHash string `json:"source_hash"`
	Page       int64  `json:"page_number"`
	Action     string `json:"action_id"`
	Unit       string `json:"unit_key"`
	Revision   string `json:"plan_revision"`
	Active     int64  `json:"active_seconds"`
	Visible    int64  `json:"visible_seconds"`
}

// ReadingPage is one aggregated (document, page) attention row.
type ReadingPage struct {
	Document string `json:"document_id"`
	Page     int64  `json:"page_number"`
	Active   int64  `json:"active_seconds"`
	Visible  int64  `json:"visible_seconds"`
}

// WorkspaceDocument is the servable reader document: current, ready, and
// pinned to a live catalog revision whose object matches the content hash.
// DisplayName and SourcePath stay encoded exactly as stored.
type WorkspaceDocument struct {
	DisplayName    string  `json:"display_name"`
	SourcePath     string  `json:"source_path"`
	SourceOrigin   string  `json:"source_origin"`
	SourceHash     string  `json:"source_hash"`
	DocumentKind   string  `json:"document_kind"`
	MimeType       *string `json:"mime_type"`
	PageCount      int64   `json:"page_count"`
	ReadingMinutes *int64  `json:"reading_minutes"`
	LanguageHint   *string `json:"language_hint"`
}

// DocumentInsightParams scopes the reader context counters. AnalysisVersion
// is the expected page-analysis generation and RequestedModel the configured
// enrichment model selecting live insight pages.
type DocumentInsightParams struct {
	Document        string `json:"document_id"`
	SourceHash      string `json:"source_hash"`
	AnalysisVersion string `json:"analysis_version"`
	RequestedModel  string `json:"requested_model"`
	CourseID        int64  `json:"course_id"`
}

// DocumentInsightCounts carries the reader context counters: live insight
// pages, non-deleted marks, and marks orphaned by source replacement.
type DocumentInsightCounts struct {
	ReadyPages  int64 `json:"ready_pages"`
	Annotations int64 `json:"annotations"`
	Orphaned    int64 `json:"orphaned"`
}

// CloseSessionParams closes one sitting. Note is already identity-encoded by
// the caller; EventID is nil when the sitting recorded no action outcome.
type CloseSessionParams struct {
	ID         int64   `json:"id"`
	Outcome    string  `json:"outcome"`
	Note       *string `json:"note"`
	Confidence *int64  `json:"confidence"`
	EventID    *int64  `json:"study_event_id"`
}

// Workspace is the typed port for the transactional reader attention and
// outcome ledger: sessions, heartbeats, reading spans and reader documents.
// Locking methods require a transaction-bound handle; backends with row locks
// hold them, SQLite serializes on its admitted writer transaction.
type Workspace interface {
	// LockVisibleCourse locks a study-visible course (visible, or hidden
	// with an enabled exam plan) and reports ErrNoRows when it is missing
	// or not visible. Mutation paths call this before taking session locks.
	LockVisibleCourse(ctx context.Context, course int64) error
	// CheckVisibleCourse reports ErrNoRows when the course is missing or
	// not study-visible, without taking row locks. Read-only snapshots
	// use this; mutation paths use LockVisibleCourse.
	CheckVisibleCourse(ctx context.Context, course int64) error
	// LockSessionKey serializes session start for one client key. It
	// requires a transaction-bound handle.
	LockSessionKey(ctx context.Context, key string) error
	// SessionByKey returns the sitting journaled under a client key. It
	// reports ErrNoRows when no sitting used the key.
	SessionByKey(ctx context.Context, key string) (WorkspaceSession, error)
	// SessionByID returns one sitting by id. It reports ErrNoRows when
	// the sitting is missing.
	SessionByID(ctx context.Context, id int64) (WorkspaceSession, error)
	// LockSession verifies the sitting's course is study-visible and
	// returns the sitting under a write lock. It reports ErrNoRows when
	// the sitting or its visible course is missing.
	LockSession(ctx context.Context, id int64) (WorkspaceSession, error)
	// InsertSession journals one sitting and returns the stored row.
	InsertSession(ctx context.Context, params InsertSessionParams) (WorkspaceSession, error)
	// BeatHash returns the stored request hash for a beat. It reports
	// ErrNoRows when the sequence was never recorded; a nil hash preserves
	// a legacy row stored without one.
	BeatHash(ctx context.Context, session, sequence int64) (*string, error)
	// InsertBeat journals one heartbeat sequence with its request hash.
	InsertBeat(ctx context.Context, session, sequence int64, hash string) error
	// ReadyDocument returns the current ready hash and page count for a
	// course document under a shared lock. It reports ErrNoRows when the
	// document is not current and ready.
	ReadyDocument(ctx context.Context, course int64, document string) (hash string, pages *int64, err error)
	// AccumulateSpan attributes one interval to the exact source revision
	// on one page, merging into the existing span row when present.
	AccumulateSpan(ctx context.Context, params AccumulateSpanParams) error
	// AddAttention adds active and visible seconds to one sitting and
	// refreshes its last-seen timestamp.
	AddAttention(ctx context.Context, session, active, visible int64) error
	// ReadingTotals aggregates attention by (document, page) in document
	// then page order. Action is already identity-encoded by the caller;
	// empty action or document disables that filter.
	ReadingTotals(ctx context.Context, course int64, action, document string) ([]ReadingPage, error)
	// WorkspaceDocument returns the servable reader document. It reports
	// ErrNoRows when the document is not current, ready, or pinned to a
	// live catalog revision.
	WorkspaceDocument(ctx context.Context, course int64, id string) (WorkspaceDocument, error)
	// DocumentPending reports a registered current document whose content
	// has not finished indexing yet (pending or running).
	DocumentPending(ctx context.Context, course int64, id string) (bool, error)
	// DocumentInsights counts live insight pages, non-deleted marks, and
	// marks orphaned by source replacement for one document revision.
	DocumentInsights(ctx context.Context, params DocumentInsightParams) (DocumentInsightCounts, error)
	// CloseSession records the outcome of one sitting and returns the
	// stored row. Action outcomes use Study.InsertStudyEvent on the same
	// transaction so the event commits or rolls back with the close.
	CloseSession(ctx context.Context, params CloseSessionParams) (WorkspaceSession, error)
}
