// Package database defines SQL-free persistence contracts and neutral values.
package database

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

// AnalysisClaim is one candidate document selected for enrichment. Fields
// carry stored values verbatim (encoded text stays encoded); ContextHash is
// the canonical Go-computed identity hash the adapters compare against the
// stored context_hash.
type AnalysisClaim struct {
	DocumentID string `json:"document_id"`
	CourseID   int64  `json:"course_id"`
}

// AnalysisDocument is the locked, eligibility-checked document tuple used for
// a claim. Stored text stays encoded exactly as stored; Pages is the stored
// page count with NULL read as zero.
type AnalysisDocument struct {
	ID         string `json:"id"`
	CourseID   int64  `json:"course_id"`
	SourceHash string `json:"source_hash"`
	Kind       string `json:"document_kind"`
	// Name, CourseName and Path stay stored-encoded; callers decode via
	// identity.Decode for prompts and resolve objects via the raw path.
	Name       string `json:"display_name"`
	CourseName string `json:"course_name"`
	Path       string `json:"normalized_path"`
	Origin     string `json:"source_origin"`
	Pages      int64  `json:"page_count"`
}

// AnalysisVersions pins the persisted analysis contract versions selecting
// live enrichment rows: document rows use SynthesisVersion for visual kinds
// and DocumentVersion otherwise; page rows always use PageVersion.
type AnalysisVersions struct {
	Document  string `json:"document_version"`
	Synthesis string `json:"synthesis_version"`
	Page      string `json:"page_version"`
}

// QueueDocumentParams files the pending document row for a claim. ContextHash
// is the canonical Go-computed identity hash; Model is both the serving
// model and the requested model; AvailableAt is the RFC3339Nano claim stamp.
type QueueDocumentParams struct {
	DocumentID  string `json:"document_id"`
	SourceHash  string `json:"source_hash"`
	ContextHash string `json:"context_hash"`
	Version     string `json:"analysis_version"`
	Model       string `json:"requested_model"`
	AvailableAt string `json:"available_at"`
}

// QueuePagesParams fans out one pending row per page for a visual document.
// Model is both the serving and requested model; AvailableAt is the
// RFC3339Nano claim stamp.
type QueuePagesParams struct {
	DocumentID  string `json:"document_id"`
	SourceHash  string `json:"source_hash"`
	Version     string `json:"analysis_version"`
	Model       string `json:"requested_model"`
	Pages       int64  `json:"page_count"`
	AvailableAt string `json:"available_at"`
}

// PageClaim is one claimed visual page. Attempts is the stored attempt count
// after this claim.
type PageClaim struct {
	Page     int64 `json:"page_number"`
	Attempts int64 `json:"attempts"`
}

// AnalysisPublishParams publishes one claimed result as ready. Model is the
// serving model that actually produced the payload (fallback output keeps the
// original Requested model); Requested selects the live generation;
// GeneratedAt is the RFC3339Nano generation stamp; Payload is the stored
// JSON text. Page is zero for document rows.
type AnalysisPublishParams struct {
	DocumentID  string `json:"document_id"`
	Page        int64  `json:"page_number"`
	ClaimedAt   string `json:"claimed_at"`
	Model       string `json:"model"`
	Requested   string `json:"requested_model"`
	Payload     string `json:"payload_json"`
	GeneratedAt string `json:"generated_at"`
	Hash        string `json:"source_hash"`
	ContextHash string `json:"context_hash"`
	Version     string `json:"analysis_version"`
}

// AnalysisFinishParams parks or fails one claimed row. AvailableAt is the
// RFC3339Nano retry stamp; Reset spends one attempt back for quota pauses and
// source-or-settings races.
type AnalysisFinishParams struct {
	DocumentID  string `json:"document_id"`
	Page        int64  `json:"page_number"`
	ClaimedAt   string `json:"claimed_at"`
	Status      string `json:"status"`
	Error       string `json:"error"`
	AvailableAt string `json:"available_at"`
	Reset       bool   `json:"reset_attempt"`
}

// ExcerptRow is one bounded chunk excerpt row. Start is empty for NULL
// locators; Text is truncated natively (2500 runes for sampled document
// excerpts, 12000 for page excerpts). Stored text stays encoded.
type ExcerptRow struct {
	LocatorType  string `json:"locator_type"`
	LocatorStart string `json:"locator_start"`
	Text         string `json:"text"`
}

// EvidenceRow is one ready page payload row. Payload is nil when over the
// 256 KiB cap.
type EvidenceRow struct {
	PageNumber int64   `json:"page_number"`
	Payload    *string `json:"payload_json"`
}

// Analysis is the typed port for document/page enrichment: candidate
// selection, durable claims, source-bound input reads, publication and
// failure handling. Claim, queue and publish methods bind to the caller's
// transaction; candidate inspection and page-image resolution use the store
// snapshot.
type Analysis interface {
	// FindCandidate selects the highest-priority enrichment-due document
	// without holding locks across I/O. It reports ErrNoRows when nothing
	// is due.
	FindCandidate(ctx context.Context, model string, versions AnalysisVersions) (AnalysisClaim, error)
	// LockCourseForClaim holds the course row through a claim. It reports
	// ErrNoRows when the course no longer exists.
	LockCourseForClaim(ctx context.Context, course int64) error
	// CurrentDocument returns the eligibility-checked document tuple,
	// locking the document row on backends with row locks. It reports
	// ErrNoRows when the document is not eligible.
	CurrentDocument(ctx context.Context, id string, lock bool) (AnalysisDocument, error)
	// QueueDocument inserts the pending document row; conflicts refresh
	// stale inputs only.
	QueueDocument(ctx context.Context, params QueueDocumentParams) error
	// ClaimDocument moves the pending document row to running and returns
	// the stored attempt count after this claim. It reports ErrNoRows when
	// the row is not pending and due at the backend clock.
	ClaimDocument(ctx context.Context, document, claimedAt string) (int64, error)
	// FailDocumentPageRange parks a visual document whose page count is
	// outside analysis limits.
	FailDocumentPageRange(ctx context.Context, document string) error
	// QueuePages fans out one pending row per page; conflicts refresh
	// stale rows only.
	QueuePages(ctx context.Context, params QueuePagesParams) error
	// ClaimPage moves the highest-priority pending due page row to running.
	// It reports ErrNoRows when no page is pending and due.
	ClaimPage(ctx context.Context, document, claimedAt string) (PageClaim, error)
	// ReadyPageCount counts ready pages of the live generation.
	ReadyPageCount(ctx context.Context, document, hash, model, version string, pages int64) (int64, error)
	// SampleExcerpts returns up to twelve evenly sampled chunk excerpts in
	// ordinal order for document-level input.
	SampleExcerpts(ctx context.Context, document string) ([]ExcerptRow, error)
	// PageExcerpts returns the page-locator chunk excerpts in ordinal order.
	PageExcerpts(ctx context.Context, document, page string) ([]ExcerptRow, error)
	// PageImageObject resolves the servable immutable object for a page
	// render. It reports ErrNoRows when the revision is deleted or absent.
	PageImageObject(ctx context.Context, document string, course int64, path, hash string) (ObjectReference, error)
	// PageEvidence lists ready page payloads in page order for a live
	// generation. Payloads over 256 KiB come back nil.
	PageEvidence(ctx context.Context, document, hash, version, model string, pages int64) ([]EvidenceRow, error)
	// PublishReady publishes one claimed row as ready, guarded by the live
	// source identity and the claim stamp.
	PublishReady(ctx context.Context, params AnalysisPublishParams) error
	// FinishClaim parks or fails one claimed row.
	FinishClaim(ctx context.Context, params AnalysisFinishParams) error
	// RecoverLane returns running document and page rows to pending,
	// spending one attempt.
	RecoverLane(ctx context.Context, availableAt string) error
}

// AnalysisContextHash renders the canonical document identity hash: sha256
// hex over the jsonb array text of the identity tuple (id, course, hash,
// kind, display name, course name, path, origin). String elements use
// PostgreSQL jsonb string escaping (short \b \f \n \r \t forms, \u00xx
// otherwise, raw UTF-8 passthrough) and the course id encodes as a JSON
// number, matching the PostgreSQL jsonb_build_array expression exactly, so
// stored context_hash values compare equal on both backends without
// recomputation.
func AnalysisContextHash(id string, course int64, hash, kind, name, courseName, path, origin string) string {
	var builder strings.Builder
	builder.WriteByte('[')
	builder.WriteString(analysisJSONString(id))
	builder.WriteString(", ")
	builder.WriteString(strconv.FormatInt(course, 10))
	for _, value := range []string{hash, kind, name, courseName, path, origin} {
		builder.WriteString(", ")
		builder.WriteString(analysisJSONString(value))
	}
	builder.WriteByte(']')
	sum := sha256.Sum256([]byte(builder.String()))
	return fmt.Sprintf("%x", sum[:])
}

func analysisJSONString(value string) string {
	var builder strings.Builder
	builder.Grow(len(value) + 2)
	builder.WriteByte('"')
	for i := range len(value) {
		c := value[i]
		switch c {
		case '"':
			builder.WriteString(`\"`)
		case '\\':
			builder.WriteString(`\\`)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(&builder, `\u%04x`, c)
			} else {
				builder.WriteByte(c)
			}
		}
	}
	builder.WriteByte('"')
	return builder.String()
}
