// Package objects defines the immutable source-reference contract shared by
// domain services. It owns no storage, network or database concerns.
package objects

import (
	"context"
	"io"

	"tree-eclass/internal/domain/database"
)

// MaxSourceBytes bounds any single source document accepted by the pipeline.
const MaxSourceBytes int64 = 50 * 1024 * 1024

// Reference pins one immutable stored object: its location, content digest and
// the version that must be served for reproducibility.
type Reference struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	SHA256    string `json:"sha256"`
	Bytes     int64  `json:"bytes"`
	MediaType string `json:"media_type"`
}

// Store is the durable object-storage contract required by domain services.
type Store interface {
	Put(ctx context.Context, input io.Reader, mediaType, tempDir string) (Reference, error)
	Download(ctx context.Context, ref Reference, temp string) (string, error)
	Open(ctx context.Context, ref Reference) (io.ReadCloser, error)
}

// RegisterObject refuses to relabel an old object ID after out-of-band object
// loss and recreation. Existing document revisions still refer to that exact version.
func RegisterObject(ctx context.Context, ops database.Operations, object Reference) error {
	return ops.Objects().RegisterObject(ctx, database.ObjectReference{
		Bucket:    object.Bucket,
		Key:       object.Key,
		VersionID: object.VersionID,
		SHA256:    object.SHA256,
		Bytes:     object.Bytes,
		MediaType: object.MediaType,
	})
}
