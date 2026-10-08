package database

import "context"

// ObjectReference pins one immutable stored object: its location, content
// digest and the version that must be served for reproducibility.
type ObjectReference struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	SHA256    string `json:"sha256"`
	Bytes     int64  `json:"bytes"`
	MediaType string `json:"media_type"`
}

// PDFDifferenceParams identifies one visual comparison: owning course, the
// two immutable input object ids, the tool version that must process it,
// and the deterministic difference identity derived by the caller.
type PDFDifferenceParams struct {
	ID          string `json:"id"`
	CourseID    int64  `json:"course_id"`
	OldObjectID string `json:"old_object_id"`
	NewObjectID string `json:"new_object_id"`
	ToolVersion string `json:"tool_version"`
}

// PDFDifference is one queued comparison with its lifecycle state. ObjectID
// is nil until a differing output publishes; Error is nil unless failed.
type PDFDifference struct {
	ID          string
	CourseID    int64
	OldObjectID string
	NewObjectID string
	ToolVersion string
	Status      string
	ObjectID    *string
	Error       *string
}

// PDFDifferencePublication links a processed output to every history row
// that references the difference. ObjectID is nil for identical inputs, in
// which case the status publishes as identical and history links clear.
// Callers register the output object via the Objects port first so the
// catalog insert and this publication commit on the same transaction.
type PDFDifferencePublication struct {
	ID       string
	ObjectID *string
}

// PDFDifferences is the typed port for the visual PDF comparison queue:
// admission, lifecycle transitions, content reads and history publication.
type PDFDifferences interface {
	// EnqueueDifference inserts one comparison idempotently and returns
	// whether this call inserted it plus the current lifecycle status.
	EnqueueDifference(ctx context.Context, params PDFDifferenceParams) (bool, string, error)
	// Difference loads one comparison by id. It reports ErrNoRows when
	// the difference does not exist.
	Difference(ctx context.Context, id string) (PDFDifference, error)
	// LockDifference loads one comparison for update on the caller's
	// transaction. It reports ErrNoRows when missing.
	LockDifference(ctx context.Context, id string) (PDFDifference, error)
	// MarkDifferenceRunning moves one comparison to running and clears
	// any previous failure message.
	MarkDifferenceRunning(ctx context.Context, id string) error
	// MarkDifferenceFailed parks a running comparison as failed with the
	// supplied message; non-running rows stay untouched.
	MarkDifferenceFailed(ctx context.Context, id, message string) error
	// PublishDifference publishes the lifecycle status for an already
	// registered output and links or clears every history row referencing
	// the difference, atomically. A nil ObjectID publishes identical.
	PublishDifference(ctx context.Context, publication PDFDifferencePublication) error
	// ReadyDifferenceObject returns the published output object for a
	// ready difference in a visible course. It reports ErrNoRows when
	// the difference is not ready, unpublished, or hidden.
	ReadyDifferenceObject(ctx context.Context, id string) (ObjectReference, error)
	// DifferenceStatus returns the current lifecycle status. It reports
	// ErrNoRows when the difference does not exist.
	DifferenceStatus(ctx context.Context, id string) (string, error)
}
