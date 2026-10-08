package queries

import (
	"context"
)

// SQLite embedding method over the driver-neutral rdbms.DBTX surface. SQL
// text is copied verbatim from the generated *.sql.go const; the sqlite
// driver rewrites placeholders/casts at Exec/Query time.

const sqliteIndexEmbedding = `-- name: IndexEmbedding :exec
INSERT INTO knowledge.chunk_embeddings(chunk_id,model,vector,dimensions) VALUES($1,$2,$3,$4)
ON CONFLICT(chunk_id,model) DO UPDATE SET vector=excluded.vector,dimensions=excluded.dimensions
`

func (q *SQLiteQueries) IndexEmbedding(ctx context.Context, arg IndexEmbeddingParams) error {
	_, err := q.db.Exec(ctx, sqliteIndexEmbedding,
		arg.ChunkID,
		arg.Model,
		arg.Vector,
		arg.Dimensions,
	)
	return err
}
