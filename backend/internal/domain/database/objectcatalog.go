package database

import "context"

// ObjectCatalog prunes abandoned content-addressed objects. Collection runs
// only while the offline controller holds every publisher stopped and owns
// the dataset, so catalog rows and stored files compare without racing writers.
type ObjectCatalog interface {
	// DeleteUnreferencedObjects deletes up to batch catalog rows carrying
	// content-addressed identity for bucket (id, key and sha256 agree in the
	// 64-character digest shape) that no catalog column holding an object id
	// references. Every foreign key to the objects catalog counts, including
	// references added by migrations newer than this release; historical
	// referencing rows count regardless of visibility or deletion markers.
	// It returns the number of catalog rows deleted.
	DeleteUnreferencedObjects(ctx context.Context, bucket string, batch int) (int64, error)

	// CatalogHasKeys reports, in input order, whether the catalog holds a row
	// for bucket with each key. It answers existence only; files stay untouched.
	CatalogHasKeys(ctx context.Context, bucket string, keys []string) ([]bool, error)
}
