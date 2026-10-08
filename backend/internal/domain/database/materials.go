package database

import "context"

// MaterialRow is one external-library document joined to its manual
// classification override. The embedded KnowledgeDocument carries every stored
// document column; MaterialType and SourceLabel are nil when no manual metadata
// row exists. Text columns stay encoded exactly as stored.
type MaterialRow struct {
	KnowledgeDocument
	MaterialType *string `json:"material_type"`
	SourceLabel  *string `json:"source_label"`
}

// MaterialMetadataParams records the manual classification for one external
// path. SourcePath is already identity-encoded by the caller.
type MaterialMetadataParams struct {
	CourseID     int64  `json:"course_id"`
	SourcePath   string `json:"source_path"`
	MaterialType string `json:"material_type"`
}

// MirrorFile is one external file pinned to its live catalog object for the
// filesystem mirror. NormalizedPath stays encoded exactly as stored.
type MirrorFile struct {
	NormalizedPath string `json:"normalized_path"`
	Bucket         string `json:"bucket"`
	Key            string `json:"key"`
	VersionID      string `json:"version_id"`
	SHA256         string `json:"sha256"`
	Bytes          int64  `json:"bytes"`
}

// MaterialPresentationParams scopes the external-library presentation read.
// Empty DocumentID selects every external document for the course;
// DocumentVersion and PageVersion are the expected analysis generations and
// Model is the configured enrichment model selecting live AI hints.
type MaterialPresentationParams struct {
	CourseID        int64  `json:"course_id"`
	DocumentID      string `json:"document_id"`
	DocumentVersion string `json:"document_version"`
	PageVersion     string `json:"page_version"`
	Model           string `json:"model"`
}

// MaterialPresentationRow is one raw presentation row. Text columns stay
// encoded exactly as stored; EnrichmentPayload holds '{}' or the live AI hint
// object for domain classification. Callers decode via identity.Decode.
type MaterialPresentationRow struct {
	ID                string  `json:"id"`
	CourseID          int64   `json:"course_id"`
	DisplayName       string  `json:"display_name"`
	SourcePath        string  `json:"source_path"`
	SourceOrigin      string  `json:"source_origin"`
	DocumentKind      string  `json:"document_kind"`
	Status            string  `json:"status"`
	DiagnosticReason  *string `json:"diagnostic_reason"`
	SourceModifiedAt  *string `json:"source_modified_at"`
	IndexedAt         *string `json:"indexed_at"`
	SourceSizeBytes   *int64  `json:"source_size_bytes"`
	PageCount         *int64  `json:"page_count"`
	ReadingMinutes    *int64  `json:"reading_minutes"`
	MaterialType      *string `json:"material_type"`
	SourceLabel       *string `json:"source_label"`
	EnrichmentPayload []byte  `json:"enrichment_payload"`
}

// Materials is the typed port for the external library: manual metadata,
// mirror pins and presentation rows. Every method binds to the caller's
// transaction or store snapshot with no implicit commits.
type Materials interface {
	// GetMaterial returns one current external document with its manual
	// metadata. It reports ErrNoRows when the document is not a current
	// external document of the course.
	GetMaterial(ctx context.Context, courseID int64, id string) (MaterialRow, error)
	// ListMaterials returns every current external document with manual
	// metadata ordered by display name then id.
	ListMaterials(ctx context.Context, courseID int64) ([]MaterialRow, error)
	// SetMaterialMetadata inserts or retypes the manual classification for
	// one external path.
	SetMaterialMetadata(ctx context.Context, params MaterialMetadataParams) error
	// ExternalMirrorFiles returns live external files pinned to catalog
	// objects ordered by normalized path. Only current external documents
	// whose live revision object matches the catalog source hash qualify;
	// archive members are excluded.
	ExternalMirrorFiles(ctx context.Context, courseID int64) ([]MirrorFile, error)
	// ListPresentation returns raw presentation rows ordered by display
	// name then id. The enrichment payload is '{}' unless the document is
	// ready with a live ready enrichment for the requested generation.
	ListPresentation(
		ctx context.Context,
		params MaterialPresentationParams,
	) ([]MaterialPresentationRow, error)
}
