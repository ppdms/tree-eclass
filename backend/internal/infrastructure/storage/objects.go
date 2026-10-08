package storage

import (
	"context"

	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
)

// RegisterObject refuses to relabel an old object ID after out-of-band object
// loss and recreation. Existing document revisions still refer to that exact version.
func RegisterObject(ctx context.Context, db rdbms.DBTX, object blob.Reference) error {
	return objects.RegisterObject(ctx, db, object)
}
