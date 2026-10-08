package database

import "context"

// KnowledgeDocument is the neutral current-document row shared by indexing
// writes and knowledge reads. JSON tags and legacy field spellings
// (SourceUrl, WarningsJson, MimeType) match the previous generated DTO so
// public API payloads do not change.
type KnowledgeDocument struct {
	ID                  string  `json:"id"`
	CourseID            int64   `json:"course_id"`
	CourseName          string  `json:"course_name"`
	CourseShortName     *string `json:"course_short_name"`
	SourcePath          string  `json:"source_path"`
	SourceOrigin        string  `json:"source_origin"`
	NormalizedPath      string  `json:"normalized_path"`
	SourceUrl           *string `json:"source_url"`
	DisplayName         string  `json:"display_name"`
	SourceHash          string  `json:"source_hash"`
	SourceFingerprint   string  `json:"source_fingerprint"`
	SourceEtag          *string `json:"source_etag"`
	ContentHashVerified int64   `json:"content_hash_verified"`
	MimeType            *string `json:"mime_type"`
	ResponseMimeType    *string `json:"response_mime_type"`
	DocumentKind        string  `json:"document_kind"`
	AcademicYear        *string `json:"academic_year"`
	SourceModifiedAt    *string `json:"source_modified_at"`
	IsCurrent           int64   `json:"is_current"`
	Status              string  `json:"status"`
	PageCount           *int64  `json:"page_count"`
	SourceSizeBytes     *int64  `json:"source_size_bytes"`
	CharacterCount      *int64  `json:"character_count"`
	WordCount           *int64  `json:"word_count"`
	ReadingMinutes      *int64  `json:"reading_minutes"`
	ComplexityScore     *int64  `json:"complexity_score"`
	ComplexityLabel     *string `json:"complexity_label"`
	LanguageHint        *string `json:"language_hint"`
	ExtractorName       *string `json:"extractor_name"`
	ExtractorVersion    *string `json:"extractor_version"`
	IndexedAt           *string `json:"indexed_at"`
	Error               *string `json:"error"`
	DiagnosticReason    *string `json:"diagnostic_reason"`
	WarningsJson        string  `json:"warnings_json"`
}

// DocumentFilter scopes admitted-document reads. Empty DocumentKinds disables
// the kind filter; empty FolderPrefix disables the prefix filter.
type DocumentFilter struct {
	CourseIDs     []int64  `json:"course_ids"`
	DocumentKinds []string `json:"document_kinds"`
	FolderPrefix  string   `json:"folder_prefix"`
}

// SearchCandidate is one admitted chunk joined to its document. Payloads stay
// encoded exactly as stored; callers decode via identity.Decode. Both backends
// return rows in (document_id, ordinal) order; no backend assigns scores.
type SearchCandidate struct {
	Document     KnowledgeDocument `json:"document"`
	ChunkID      string            `json:"chunk_id"`
	Ordinal      int64             `json:"ordinal"`
	LocatorType  string            `json:"locator_type"`
	LocatorStart *string           `json:"locator_start"`
	LocatorEnd   *string           `json:"locator_end"`
	Heading      *string           `json:"heading"`
	MetadataJSON string            `json:"metadata_json"`
	Excerpt      string            `json:"excerpt"`
	Text         string            `json:"text"`
}

// EmbeddedCandidate pairs a search candidate with its packed embedding vector.
type EmbeddedCandidate struct {
	SearchCandidate
	Vector []byte `json:"-"`
}

// DocumentChunk is one ordered chunk row for the material reader.
type DocumentChunk struct {
	ID           string  `json:"id"`
	Ordinal      int64   `json:"ordinal"`
	LocatorType  string  `json:"locator_type"`
	LocatorStart *string `json:"locator_start"`
	LocatorEnd   *string `json:"locator_end"`
	Heading      *string `json:"heading"`
	MetadataJSON string  `json:"metadata_json"`
	Text         string  `json:"text"`
}

// ChunkLocator is the ordinal/locator triple used for locator matching.
type ChunkLocator struct {
	Ordinal      int64  `json:"ordinal"`
	LocatorType  string `json:"locator_type"`
	LocatorStart string `json:"locator_start"`
}

// ContentObject pins the servable immutable object plus its display name.
type ContentObject struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	SHA256    string `json:"sha256"`
	Bytes     int64  `json:"bytes"`
	MediaType string `json:"media_type"`
	Name      string `json:"name"`
}

// DocumentEnrichment is the current document-enrichment row joined to its
// document kind and live hash. Payload is nil when over the size cap or absent.
type DocumentEnrichment struct {
	Status       string  `json:"status"`
	SourceHash   string  `json:"source_hash"`
	Model        string  `json:"model"`
	Requested    string  `json:"requested_model"`
	Version      string  `json:"analysis_version"`
	Payload      *string `json:"payload_json"`
	GeneratedAt  *string `json:"generated_at"`
	DocumentKind string  `json:"document_kind"`
	CurrentHash  string  `json:"current_hash"`
}

// PageEnrichment is one page-enrichment row. Payload is nil when over the
// caller's size cap.
type PageEnrichment struct {
	PageNumber  int64   `json:"page_number"`
	Status      string  `json:"status"`
	Model       string  `json:"model"`
	GeneratedAt *string `json:"generated_at"`
	SourceHash  string  `json:"source_hash"`
	Payload     *string `json:"payload_json"`
	Version     string  `json:"analysis_version"`
	Requested   string  `json:"requested_model"`
}

// PageAnalysisParams selects one exact page-enrichment row.
type PageAnalysisParams struct {
	Document string `json:"document_id"`
	Page     int64  `json:"page_number"`
	Hash     string `json:"source_hash"`
	Version  string `json:"analysis_version"`
	Model    string `json:"requested_model"`
	MaxBytes int64  `json:"max_bytes"`
}

// PageCoverageParams selects the page-enrichment rows aggregated by status.
type PageCoverageParams struct {
	Document string `json:"document_id"`
	Hash     string `json:"source_hash"`
	Version  string `json:"analysis_version"`
	Model    string `json:"requested_model"`
}

// PageStatusCount aggregates page rows by status with a representative model.
type PageStatusCount struct {
	Status string `json:"status"`
	Count  int64  `json:"count"`
	Model  string `json:"model"`
}

// RelatedMaterial is one resolved display row in request order.
type RelatedMaterial struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

// FileMetadataParams selects compact file-guide rows for one course.
type FileMetadataParams struct {
	Course           int64  `json:"course_id"`
	Model            string `json:"model"`
	PageVersion      string `json:"page_version"`
	DocumentVersion  string `json:"document_version"`
	SynthesisVersion string `json:"synthesis_version"`
}

// FileMetadataRow is one compact file-guide row. SummaryReady reports the
// payload's usable-summary predicate evaluated natively per backend.
type FileMetadataRow struct {
	ID            string  `json:"id"`
	Path          string  `json:"path"`
	Hash          string  `json:"source_hash"`
	Kind          string  `json:"document_kind"`
	Status        string  `json:"status"`
	Reason        *string `json:"reason"`
	Error         *string `json:"error"`
	Pages         *int64  `json:"page_count"`
	Minutes       *int64  `json:"reading_minutes"`
	Complexity    *string `json:"complexity_label"`
	Analysis      string  `json:"analysis_status"`
	Model         *string `json:"model"`
	Version       *string `json:"version"`
	GeneratedAt   *string `json:"generated_at"`
	AnalysisError *string `json:"analysis_error"`
	Guide         bool    `json:"guide_available"`
	PagesReady    int64   `json:"pages_ready"`
	PagesTotal    int64   `json:"pages_total"`
}

// CoverageRow aggregates per-course document status counts.
type CoverageRow struct {
	CourseID  int64 `json:"course_id"`
	Supported int64 `json:"supported_documents"`
	Indexed   int64 `json:"indexed_documents"`
	Failed    int64 `json:"failed_documents"`
	Pending   int64 `json:"pending_documents"`
}

// StatusCount aggregates rows by status.
type StatusCount struct {
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

// DiagnosticDocument is one failed/unsupported document row for diagnostics.
type DiagnosticDocument struct {
	DocumentID string  `json:"document_id"`
	CourseID   int64   `json:"course_id"`
	Display    string  `json:"display_name"`
	Path       string  `json:"source_path"`
	Status     string  `json:"status"`
	Reason     *string `json:"diagnostic_reason"`
	Error      *string `json:"error"`
}

// GuideFreshnessParams selects guide freshness rows for visible courses.
type GuideFreshnessParams struct {
	Courses          []int64 `json:"courses"`
	Model            string  `json:"model"`
	DocumentVersion  string  `json:"document_version"`
	SynthesisVersion string  `json:"synthesis_version"`
}

// GuideDiagnostic is one non-ready guide row with its freshness reason.
type GuideDiagnostic struct {
	DocumentID string  `json:"document_id"`
	CourseID   int64   `json:"course_id"`
	Display    string  `json:"display_name"`
	Path       string  `json:"source_path"`
	Model      *string `json:"model"`
	Error      *string `json:"error"`
	Status     string  `json:"status"`
	Reason     string  `json:"reason"`
}

// ReadinessParams selects readiness counters for one course.
type ReadinessParams struct {
	Course           int64  `json:"course_id"`
	Model            string `json:"model"`
	DocumentVersion  string `json:"document_version"`
	SynthesisVersion string `json:"synthesis_version"`
	PageVersion      string `json:"page_version"`
}

// ReadinessCount is one (category, status, count) readiness counter row.
type ReadinessCount struct {
	Category string `json:"category"`
	Status   string `json:"status"`
	Count    int64  `json:"count"`
}

// ChangeItem is one change-feed row. Text columns stay encoded as stored,
// except CourseName and Timestamp which are plain.
type ChangeItem struct {
	CourseID    int64   `json:"course_id"`
	CourseName  string  `json:"course_name"`
	Timestamp   string  `json:"timestamp"`
	ChangeNo    string  `json:"change_no"`
	ChangeType  string  `json:"change_type"`
	FilePath    *string `json:"file_path"`
	DisplayName *string `json:"display_name"`
	RedirectURL *string `json:"redirect_url"`
	DiffWebdav  *string `json:"diff_webdav_path"`
}

// AdminDocument pairs a document with chunk/embedding counts for diagnostics.
type AdminDocument struct {
	KnowledgeDocument
	ChunkCount     int64 `json:"chunk_count"`
	EmbeddingCount int64 `json:"embedding_count"`
}

// Documents is the typed read port for admitted knowledge documents, chunks,
// enrichments, diagnostics and content objects. All source-boundary admission
// (catalog membership, current content identity, archive ancestry) is enforced
// inside the native implementations; callers never express it.
//
// Lexical and semantic operations return unscored candidates in deterministic
// (document_id, ordinal) order. Domain Go owns cosine similarity, metadata
// bonuses and reciprocal-rank fusion over identical inputs. Lexical matching
// applies the shared Unicode normalizer to source text and metadata, never
// serialized search-vector positions or a backend-specific relevance score.
type Documents interface {
	// VisibleCourseIDs lists visible course IDs ascending.
	VisibleCourseIDs(ctx context.Context) ([]int64, error)
	// CourseVisible reports whether one course is visible (not hidden).
	CourseVisible(ctx context.Context, course int64) (bool, error)
	// GetReadableDocument returns the admitted current visible document by ID.
	GetReadableDocument(ctx context.Context, id string) (KnowledgeDocument, error)
	// DocumentAdmitted reports whether a document passes the source boundary.
	DocumentAdmitted(ctx context.Context, id string) (bool, error)
	// ListMaterials pages admitted current documents by id, returning at most limit+1.
	// Since compares instants, independent of timezone offset or fractional formatting.
	ListMaterials(ctx context.Context, course int64, cursor, prefix, kind string, since *string,
		limit int) ([]KnowledgeDocument, error)
	// ListAdminDocuments lists documents in the supplied visible-course scope with chunk counts.
	// Query matches display name or source path using Unicode case-insensitive substring matching.
	ListAdminDocuments(ctx context.Context, courses []int64, status, query string, limit int) ([]AdminDocument, error)
	// LexicalCandidates returns unscored admitted chunk candidates matching
	// every lowered term, in (document_id, ordinal) order, bounded by limit.
	LexicalCandidates(ctx context.Context, filter DocumentFilter, terms []string, limit int) ([]SearchCandidate, error)
	// EmbeddedCandidates streams unscored admitted chunk candidates with
	// packed vectors in (document_id, ordinal) order. Callers bound the scan
	// with a heap; the iterator must be closed.
	EmbeddedCandidates(ctx context.Context, filter DocumentFilter, model string,
		dimensions int) (Iterator[EmbeddedCandidate], error)
	// ReadChunks returns ordered chunks for a document; all selects every
	// chunk, otherwise only the given ordinals.
	ReadChunks(ctx context.Context, document string, ordinals []int64, all bool) ([]DocumentChunk, error)
	// ChunkLocators lists ordinal/locator triples ordered by ordinal.
	ChunkLocators(ctx context.Context, document string) ([]ChunkLocator, error)
	// GuideNavigation returns admitted materials (normalized_path, id order,
	// max 100) and distinct headings (max 100) for one course.
	GuideNavigation(ctx context.Context, course int64) (materials, headings []string, err error)
	// ContentObject resolves the servable object for a document; revision
	// selects a historic revision, empty selects the current hash.
	ContentObject(ctx context.Context, course int64, document, revision string) (ContentObject, error)
	// LogicalContentObject resolves the servable object for a logical path
	// and returns the stored logical path alongside.
	LogicalContentObject(ctx context.Context, logical string) (ContentObject, string, error)
	// DocumentAnalysis reads the current document-enrichment row, if any.
	DocumentAnalysis(ctx context.Context, id string) (DocumentEnrichment, error)
	// PageEnrichments reads page rows for a range in page order. maxBytes
	// caps the returned payload; oversized payloads come back nil.
	PageEnrichments(ctx context.Context, document string, first, last int64, maxBytes int64) ([]PageEnrichment, error)
	// PageAnalysis reads one exact page-enrichment row, if any.
	PageAnalysis(ctx context.Context, params PageAnalysisParams) (PageEnrichment, error)
	// PageCoverage aggregates page rows by status for a document generation.
	PageCoverage(ctx context.Context, params PageCoverageParams) ([]PageStatusCount, error)
	// RelatedMaterials resolves ready display rows in request-path order.
	RelatedMaterials(ctx context.Context, course int64, paths []string) ([]RelatedMaterial, error)
	// ReadyDocumentHash returns the admitted ready hash and page count for a
	// visible (or exam-planned) course document.
	ReadyDocumentHash(ctx context.Context, course int64, document string) (hash string, pages *int64, err error)
	// FileGuideHash returns the ready current visible hash for a document.
	FileGuideHash(ctx context.Context, course int64, document string) (string, error)
	// FileMetadataRows lists compact file-guide rows in (source_path, id).
	FileMetadataRows(ctx context.Context, params FileMetadataParams) ([]FileMetadataRow, error)
	// Coverage aggregates per-course status counts, optionally for one course.
	Coverage(ctx context.Context, course *int64) ([]CoverageRow, error)
	// StatusCoverage aggregates status-dashboard rows for visible courses.
	StatusCoverage(ctx context.Context, courses []int64) ([]CoverageRow, error)
	// StatusCounts aggregates current-document counts by status.
	StatusCounts(ctx context.Context, courses []int64) ([]StatusCount, error)
	// IndexJobCounts aggregates extraction job counts by status.
	IndexJobCounts(ctx context.Context, courses []int64) ([]StatusCount, error)
	// FailedDocuments lists failed current documents in (course, path) order.
	FailedDocuments(ctx context.Context, courses []int64, limit int) ([]DiagnosticDocument, error)
	// UnsupportedDocuments lists unsupported/skipped documents, same order.
	UnsupportedDocuments(ctx context.Context, courses []int64, limit int) ([]DiagnosticDocument, error)
	// GuideSummary aggregates guide freshness statuses.
	GuideSummary(ctx context.Context, params GuideFreshnessParams) ([]StatusCount, error)
	// GuideDiagnostics lists non-ready guide rows in (course, path) order.
	GuideDiagnostics(ctx context.Context, params GuideFreshnessParams, limit int) ([]GuideDiagnostic, error)
	// EmbeddingCounts counts chunks and embedded chunks for admitted docs.
	EmbeddingCounts(ctx context.Context, courses []int64, model string) (chunks, embedded int64, err error)
	// ReadinessCounts aggregates readiness counters for one course.
	ReadinessCounts(ctx context.Context, params ReadinessParams) ([]ReadinessCount, error)
	// BlueprintStatus returns the latest blueprint status or "missing".
	BlueprintStatus(ctx context.Context, course int64, model, version string) (string, error)
	// RecentChanges lists change-feed items in (timestamp, id) desc order.
	RecentChanges(ctx context.Context, courses []int64, since string, limit int) ([]ChangeItem, error)
}
