package queries

import (
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"tree-eclass/internal/infrastructure/rdbms"
)

// SQLiteQueries implements the same 35 sqlc query methods as Queries but over
// the driver-neutral rdbms.DBTX surface so the same call sites run on sqlite.
// SQL text is copied verbatim from the generated *.sql.go consts ($N
// placeholders, schema-qualified names, casts); the sqlite driver rewrites
// placeholders/casts/row locks at Exec/Query time.
type SQLiteQueries struct {
	db rdbms.DBTX
}

// NewSQLiteQueries builds a SQLiteQueries over any rdbms pool or transaction.
func NewSQLiteQueries(db rdbms.DBTX) *SQLiteQueries {
	return &SQLiteQueries{db: db}
}

// parseTimestamptz converts sqlite TEXT timestamps into pgtype.Timestamptz.
// Shared by all sqlite_*.go files in this package; do not redefine elsewhere.
func parseTimestamptz(ns sql.NullString) pgtype.Timestamptz {
	if !ns.Valid || ns.String == "" {
		return pgtype.Timestamptz{}
	}
	s := ns.String
	formats := []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return pgtype.Timestamptz{Time: t, Valid: true}
		}
	}
	return pgtype.Timestamptz{}
}
