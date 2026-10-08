package knowledge

import (
	"context"
	"path"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
)

type Content struct {
	Object objects.Reference
	Name   string
}

// Content admits the exact immutable version and current archive ancestry in
// one snapshot. An explicit historic revision never authorizes a different
// document or course, and pending/stale current sources cannot become readers.
func (s Reader) Content(ctx context.Context, course int64, document, revision string) (Content, error) {
	row, err := s.Pool.Documents().ContentObject(ctx, course, document, revision)
	if err != nil {
		return Content{}, err
	}
	return Content{
		Object: objects.Reference{
			Bucket: row.Bucket, Key: row.Key, VersionID: row.VersionID,
			SHA256: row.SHA256, Bytes: row.Bytes, MediaType: row.MediaType,
		},
		Name: identity.Decode(row.Name),
	}, nil
}

// LogicalContent preserves explicit historical file-version paths while current
// compatibility paths obey the same source admission as the id-based reader.
func (s Reader) LogicalContent(ctx context.Context, logical string) (Content, error) {
	row, name, err := s.Pool.Documents().LogicalContentObject(ctx, logical)
	if err != nil {
		return Content{}, err
	}
	return Content{
		Object: objects.Reference{
			Bucket: row.Bucket, Key: row.Key, VersionID: row.VersionID,
			SHA256: row.SHA256, Bytes: row.Bytes, MediaType: row.MediaType,
		},
		Name: path.Base(identity.Decode(name)),
	}, nil
}
