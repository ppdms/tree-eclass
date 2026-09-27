package storage

import (
	"context"

	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/blob"
)

// RegisterObject refuses to relabel an old object ID after out-of-band S3 loss
// and recreation. Existing document revisions still refer to that exact version.
func RegisterObject(ctx context.Context, db queries.DBTX, object blob.Reference) error {
	return objects.RegisterObject(ctx, db, object)
}
