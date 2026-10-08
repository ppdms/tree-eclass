package database

import "context"

// DocumentObjectParams selects one servable immutable object. Empty Revision
// selects the current revision whose object matches the live catalog source
// hash; a non-empty Revision selects that exact historic revision.
type DocumentObjectParams struct {
	DocumentID string `json:"document_id"`
	Revision   string `json:"revision"`
}

// RegisterRevisionParams stages one immutable revision. ID is the caller's
// deterministic revision identity; ObjectID is the catalog object id.
type RegisterRevisionParams struct {
	ID          string `json:"id"`
	DocumentID  string `json:"document_id"`
	CourseID    int64  `json:"course_id"`
	LogicalPath string `json:"logical_path"`
	ObjectID    string `json:"object_id"`
}

// DocumentObject pins one servable immutable object to its stored revision.
// CreatedAt is the neutral object registration instant; LogicalPath stays
// stored-encoded where the caller encoded it.
type DocumentObject struct {
	Object      ObjectReference `json:"object"`
	CreatedAt   OptionalTime    `json:"created_at"`
	LogicalPath string          `json:"logical_path"`
	CourseID    int64           `json:"course_id"`
}

// Objects is the typed port for the immutable object catalog and its
// revision pins. Every method binds to the caller's transaction or store
// snapshot with no implicit commits.
type Objects interface {
	// RegisterObject inserts one catalog object idempotently. An identical
	// row is a no-op; a conflicting row for the same id reports a
	// mismatch error and never relabels the stored version.
	RegisterObject(ctx context.Context, object ObjectReference) error
	// RegisterRevision stages one document revision idempotently. A
	// repeated (document, object) pair is a no-op.
	RegisterRevision(ctx context.Context, params RegisterRevisionParams) error
	// DocumentObject resolves the servable object for a document. Empty
	// revision follows the live catalog source hash; an explicit revision
	// never authorizes a different document. It reports ErrNoRows when
	// the revision is deleted or absent.
	DocumentObject(ctx context.Context, params DocumentObjectParams) (DocumentObject, error)
	// FileObject resolves the servable object for one revision in a
	// visible course. It reports ErrNoRows when the revision is deleted,
	// absent, or its course is hidden.
	FileObject(ctx context.Context, revisionID string) (DocumentObject, error)
	// GetObject returns one catalog object by id. It reports ErrNoRows
	// when the id is unknown.
	GetObject(ctx context.Context, id string) (ObjectReference, error)
}
