// Package objectgc removes abandoned objects only while the controller excludes
// all application writers and holds the dataset operation lock. It is never an
// online worker: a timed grace period cannot protect an in-flight publication.
package objectgc

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

type Result struct {
	CatalogRows int64 `json:"catalog_rows_removed"`
	Versions    int64 `json:"object_versions_removed"`
	Bytes       int64 `json:"logical_bytes_removed"`
}

func Collect(ctx context.Context, db *storage.Database, store *blob.Store) (Result, error) {
	return CollectOperations(ctx, db, store, db.Pool.ObjectCatalog())
}

// CollectOperations runs collection against a typed object catalog. The store
// handle still probes dataset ownership between batches; catalog pruning and
// key checks go through the typed catalog.
func CollectOperations(
	ctx context.Context,
	db *storage.Database,
	store *blob.Store,
	catalog database.ObjectCatalog,
) (Result, error) {
	var result Result
	for {
		if err := db.CheckOwner(ctx); err != nil {
			return result, err
		}
		removed, err := catalog.DeleteUnreferencedObjects(ctx, blob.DataBucket, 500)
		if err != nil {
			return result, err
		}
		result.CatalogRows += removed
		if removed == 0 {
			break
		}
	}
	sweep, err := store.PruneUnregistered(ctx, func(ctx context.Context, keys, _ []string) ([]bool, error) {
		if err := db.CheckOwner(ctx); err != nil {
			return nil, err
		}
		return catalog.CatalogHasKeys(ctx, blob.DataBucket, keys)
	})
	result.Versions, result.Bytes = sweep.Versions, sweep.Bytes
	return result, err
}
