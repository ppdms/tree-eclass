package workflow

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
)

// fixtureStore pairs a typed Store with its native handle for fixture-only
// seed and assertion SQL. Production contracts stay SQL-free; tests keep
// raw database/sql access through the explicit Native handle.
type fixtureStore struct {
	database.Store
	Native *sql.DB
}

// newFixtureStore opens the controller's sqlite database and wraps it in the
// typed Store. The caller owns Close via the returned Store; closing the
// Store also closes Native.
func newFixtureStore(t *testing.T, ctx context.Context, c *Controller) *fixtureStore {
	t.Helper()
	native, err := sql.Open("sqlite", sqliteDSN(c.storageConfig().SQLitePath))
	if err != nil {
		t.Fatal(err)
	}
	if err = native.PingContext(ctx); err != nil {
		_ = native.Close()
		t.Fatal(err)
	}
	return &fixtureStore{Store: rdbms.WrapSQLite(native), Native: native}
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

// Close releases the typed Store, then the native handle it borrows.
func (f *fixtureStore) Close() {
	f.Store.Close()
	_ = f.Native.Close()
}
