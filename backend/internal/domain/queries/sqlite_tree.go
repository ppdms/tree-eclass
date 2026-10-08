package queries

import (
	"context"
)

// SQLite tree methods over the driver-neutral rdbms.DBTX surface. SQL text
// is copied verbatim from the generated *.sql.go consts; the sqlite driver
// rewrites placeholders/casts at Exec/Query time.

const sqliteTreeFiles = `-- name: TreeFiles :many
SELECT f.node_id,f.url,f.name,f.md5_hash,f.etag,f.last_updated,f.local_path,f.redirect_url
FROM app.files f JOIN app.nodes n ON n.id=f.node_id WHERE n.course_id=$1 ORDER BY f.id
`

func (q *SQLiteQueries) TreeFiles(ctx context.Context, courseID int64) ([]TreeFilesRow, error) {
	rows, err := q.db.Query(ctx, sqliteTreeFiles, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []TreeFilesRow{}
	for rows.Next() {
		var i TreeFilesRow
		if err := rows.Scan(
			&i.NodeID,
			&i.Url,
			&i.Name,
			&i.Md5Hash,
			&i.Etag,
			&i.LastUpdated,
			&i.LocalPath,
			&i.RedirectUrl,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const sqliteTreeNodes = `-- name: TreeNodes :many
SELECT id,parent_id,name,url,local_path FROM app.nodes WHERE course_id=$1 ORDER BY id
`

func (q *SQLiteQueries) TreeNodes(ctx context.Context, courseID int64) ([]TreeNodesRow, error) {
	rows, err := q.db.Query(ctx, sqliteTreeNodes, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []TreeNodesRow{}
	for rows.Next() {
		var i TreeNodesRow
		if err := rows.Scan(
			&i.ID,
			&i.ParentID,
			&i.Name,
			&i.Url,
			&i.LocalPath,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
