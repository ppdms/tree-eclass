package queries

import (
	"context"
	"database/sql"
)

// SQLite activity helpers: small null/initcap utilities shared by the
// activity page query and assembly in sqlite_activitypage.go.

func activityNullString(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.String
	return &s
}

func activityNullInt64(nn sql.NullInt64) *int64 {
	if !nn.Valid {
		return nil
	}
	n := nn.Int64
	return &n
}

// sqliteActivityPageQuery selects the event page with the same UNION ALL
// shape and ordering as the postgres ActivityPage query (hidden courses
// excluded, stamp DESC NULLS LAST, kind, id DESC).
func (q *SQLiteQueries) sqliteActivityPageQuery(
	ctx context.Context, arg ActivityPageParams,
) ([]sqliteActivityEvent, error) {
	rows, err := q.db.Query(ctx, sqliteActivityPageSQL, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []sqliteActivityEvent
	for rows.Next() {
		var e sqliteActivityEvent
		var stamp sql.NullString
		var courseID sql.NullInt64
		if err := rows.Scan(&stamp, &e.id, &e.kind, &courseID); err != nil {
			return nil, err
		}
		e.stamp = activityNullString(stamp)
		e.courseID = activityNullInt64(courseID)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
