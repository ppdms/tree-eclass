package workflow

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
)

// fixtureStore pairs a typed Store with its native handle for fixture-only
// seed and assertion SQL. Production contracts stay SQL-free; tests keep
// raw pgx access through the explicit Native handle.
type fixtureStore struct {
	database.Store
	Native *pgxpool.Pool
}

// newFixtureStore wraps an admitted native pool. The caller owns Close via
// the returned Store; closing the Store also closes Native.
func newFixtureStore(t *testing.T, ctx context.Context, c *Controller) *fixtureStore {
	t.Helper()
	native, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	return &fixtureStore{Store: rdbms.WrapPostgres(native), Native: native}
}

// asObjectReference converts a fixture blob reference to its neutral catalog
// shape. Both carry identical bucket/key/version/sha/bytes/media fields.
func asObjectReference(ref blob.Reference) database.ObjectReference {
	return database.ObjectReference{
		Bucket:    ref.Bucket,
		Key:       ref.Key,
		VersionID: ref.VersionID,
		SHA256:    ref.SHA256,
		Bytes:     ref.Bytes,
		MediaType: ref.MediaType,
	}
}

// Close releases the typed Store, which owns the native pool.
func (f *fixtureStore) Close() {
	f.Store.Close()
}
