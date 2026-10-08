package database

import "context"

// ObserveDocumentParams admits one pending external document. SourcePath,
// NormalizedPath and DisplayName are already identity-encoded by the caller.
type ObserveDocumentParams struct {
	ID              string  `json:"id"`
	CourseID        int64   `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
	SourcePath      string  `json:"source_path"`
	NormalizedPath  string  `json:"normalized_path"`
	DisplayName     string  `json:"display_name"`
	SourceHash      string  `json:"source_hash"`
	MimeType        *string `json:"mime_type"`
	DocumentKind    string  `json:"document_kind"`
	SourceSizeBytes *int64  `json:"source_size_bytes"`
}

// EclassDocumentParams upserts one eClass document revision. Path and Name
// are already identity-encoded by the caller.
type EclassDocumentParams struct {
	Document        string  `json:"document_id"`
	CourseID        int64   `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
	Path            string  `json:"source_path"`
	Name            string  `json:"display_name"`
	SHA             string  `json:"source_hash"`
	URL             *string `json:"source_url"`
	ETag            *string `json:"source_etag"`
	Media           *string `json:"mime_type"`
	Kind            string  `json:"document_kind"`
	Bytes           *int64  `json:"source_size_bytes"`
	Status          string  `json:"status"`
	Updated         *string `json:"source_modified_at"`
}

// InsertChunkParams inserts one derived chunk row. LocatorStart, LocatorEnd
// and Heading are optional; Text and NormalizedText are already
// identity-encoded by the caller. MetadataJSON holds the stored JSON object.
type InsertChunkParams struct {
	ID             string  `json:"id"`
	DocumentID     string  `json:"document_id"`
	Ordinal        int64   `json:"ordinal"`
	LocatorType    string  `json:"locator_type"`
	LocatorStart   *string `json:"locator_start"`
	LocatorEnd     *string `json:"locator_end"`
	Heading        *string `json:"heading"`
	Text           string  `json:"text"`
	NormalizedText string  `json:"normalized_text"`
	ContentHash    string  `json:"content_hash"`
	MetadataJSON   string  `json:"metadata_json"`
}

// IndexChunkSearchParams inserts one chunk full-text row. All text fields
// are already identity-encoded by the caller where encoding applies. The
// backend trigger maintains the native search vector.
type IndexChunkSearchParams struct {
	ChunkID        string  `json:"chunk_id"`
	Text           *string `json:"text"`
	NormalizedText *string `json:"normalized_text"`
	Heading        *string `json:"heading"`
	DisplayName    *string `json:"display_name"`
	SourcePath     *string `json:"source_path"`
	CourseName     *string `json:"course_name"`
}

// IndexEmbeddingParams upserts one packed chunk embedding vector.
type IndexEmbeddingParams struct {
	ChunkID    string `json:"chunk_id"`
	Model      string `json:"model"`
	Vector     []byte `json:"vector"`
	Dimensions int64  `json:"dimensions"`
}

// MarkIndexedParams publishes extraction results for one document.
// IndexedAt carries the extractor timestamp text. WarningsJSON holds the
// stored JSON array.
type MarkIndexedParams struct {
	ID              string  `json:"id"`
	PageCount       *int64  `json:"page_count"`
	CharacterCount  *int64  `json:"character_count"`
	WordCount       *int64  `json:"word_count"`
	ReadingMinutes  *int64  `json:"reading_minutes"`
	IndexedAt       *string `json:"indexed_at"`
	WarningsJSON    string  `json:"warnings_json"`
	ComplexityScore *int64  `json:"complexity_score"`
	ComplexityLabel *string `json:"complexity_label"`
}

// RecordIndexFailureParams parks or re-queues a document after extraction
// fails. Hash must match the running revision; Error is already
// identity-encoded by the caller.
type RecordIndexFailureParams struct {
	ID     string `json:"id"`
	Hash   string `json:"source_hash"`
	Status string `json:"status"`
	Error  string `json:"error"`
	Reason string `json:"diagnostic_reason"`
}

// StartIndexRunParams marks one admitted document running. Hash must match
// the claimed revision; a mismatch reports ErrNoRows.
type StartIndexRunParams struct {
	ID   string `json:"id"`
	Hash string `json:"source_hash"`
}

// ArchiveDocumentParams upserts one archive member document. Path and Name
// are already identity-encoded by the caller.
type ArchiveDocumentParams struct {
	ID              string  `json:"id"`
	CourseID        int64   `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
	Path            string  `json:"source_path"`
	Name            string  `json:"display_name"`
	SHA             string  `json:"source_hash"`
	Media           string  `json:"mime_type"`
	Kind            string  `json:"document_kind"`
	Bytes           int64   `json:"source_size_bytes"`
	SourceOrigin    string  `json:"source_origin"`
}

// ArchiveMemberParams upserts one archive membership row. Path carries the
// encoded member path and ChainJSON the stored JSON chain.
type ArchiveMemberParams struct {
	ChildDocumentID  string `json:"child_document_id"`
	ParentDocumentID string `json:"parent_document_id"`
	Path             string `json:"member_path"`
	ChainJSON        string `json:"member_chain_json"`
	Depth            int    `json:"depth"`
	ArchiveFormat    string `json:"archive_format"`
	ParentHash       string `json:"parent_source_hash"`
	ParentPrint      string `json:"parent_source_fingerprint"`
	MemberHash       string `json:"member_hash"`
	CRC32            int64  `json:"crc32"`
	CompressedSize   int64  `json:"compressed_size"`
	ExpandedSize     int64  `json:"expanded_size"`
	MemberKind       string `json:"member_kind"`
	Media            string `json:"mime_type"`
}

// RequeueDocumentParams re-pends one document revision for extraction.
type RequeueDocumentParams struct {
	ID    string `json:"id"`
	Hash  string `json:"source_hash"`
	Bytes int64  `json:"source_size_bytes"`
}

// Indexing is the typed port for extraction admission, derived index
// publication, source failure, derived index maintenance and archive
// member publication. Every method binds to the caller's transaction or
// store snapshot with no implicit commits.
type Indexing interface {
	// IndexDocument returns the current document by id. It reports
	// ErrNoRows when the document is not current.
	IndexDocument(ctx context.Context, id string) (KnowledgeDocument, error)
	// ObserveDocument inserts one pending external document.
	ObserveDocument(ctx context.Context, params ObserveDocumentParams) error
	// ObserveEclassDocument upserts one eClass document and returns its status.
	ObserveEclassDocument(ctx context.Context, params EclassDocumentParams) (string, error)
	// StartIndexRun marks one admitted document running. A hash or
	// currency mismatch reports ErrNoRows.
	StartIndexRun(ctx context.Context, params StartIndexRunParams) error
	// RecordIndexFailure parks a running document as failed (or re-queues
	// it as pending after interruption). Only the matching running
	// revision transitions.
	RecordIndexFailure(ctx context.Context, params RecordIndexFailureParams) error
	// LockCourseForIndex locks the course row for extraction publication.
	// It reports ErrNoRows when the course is missing.
	LockCourseForIndex(ctx context.Context, courseID int64) error
	// LockDocumentForIndex locks the document row for publication. It
	// reports ErrNoRows when the document is missing.
	LockDocumentForIndex(ctx context.Context, id string) error
	// ReplaceChunks deletes every derived chunk row for one document.
	// Search and embedding rows cascade.
	ReplaceChunks(ctx context.Context, documentID string) error
	// InsertChunk inserts one derived chunk row.
	InsertChunk(ctx context.Context, params InsertChunkParams) error
	// IndexChunkSearch inserts one chunk full-text row.
	IndexChunkSearch(ctx context.Context, params IndexChunkSearchParams) error
	// IndexEmbedding upserts one packed chunk embedding vector.
	IndexEmbedding(ctx context.Context, params IndexEmbeddingParams) error
	// MarkIndexed publishes extraction results and marks the document
	// ready.
	MarkIndexed(ctx context.Context, params MarkIndexedParams) error
	// UpsertArchiveDocument upserts one archive member document and
	// restores its explicit revision. It returns the resulting status.
	UpsertArchiveDocument(ctx context.Context, params ArchiveDocumentParams) (string, error)
	// UpsertArchiveMember upserts one archive membership row.
	UpsertArchiveMember(ctx context.Context, params ArchiveMemberParams) error
	// RetireMissingArchiveMembers marks current member documents absent
	// from the latest scan as not current. Member history rows stay.
	RetireMissingArchiveMembers(ctx context.Context, parent string, current []string) error
	// RequeueDocument re-pends one document revision for extraction.
	RequeueDocument(ctx context.Context, params RequeueDocumentParams) error
	// LockCoursesForMaintenance orders and locks every course row for a
	// maintenance publish on the caller's transaction.
	LockCoursesForMaintenance(ctx context.Context) error
	// MaintainIndex re-queues extraction candidates for one maintenance
	// action: reconcile, rebuild or retry_failed. Unknown actions report
	// an error without touching any row.
	MaintainIndex(ctx context.Context, action string) error
	// RetryFailedAnalyses re-queues failed enrichment lanes after a
	// retry_failed maintenance publish.
	RetryFailedAnalyses(ctx context.Context) error
}
