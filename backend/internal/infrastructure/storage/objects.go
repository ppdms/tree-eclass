package storage

import (
	"context"
	"errors"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage/queries"
)

// RegisterObject refuses to relabel an old object ID after out-of-band S3 loss
// and recreation. Existing document revisions still refer to that exact version.
func RegisterObject(ctx context.Context, db queries.DBTX, object blob.Reference) error {
	n, err := queries.New(db).
		RegisterObject(
			ctx,
			queries.RegisterObjectParams{
				ID:        object.SHA256,
				Bucket:    object.Bucket,
				Key:       object.Key,
				VersionID: object.VersionID,
				Sha256:    object.SHA256,
				Bytes:     object.Bytes,
				MediaType: object.MediaType,
			},
		)
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New(
			"object catalog identity differs from S3; restore or reconcile the stored revision before publishing",
		)
	}
	return nil
}
