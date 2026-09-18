package study

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

type InboxItem struct {
	Path       string  `json:"file_path"`
	Name       string  `json:"file_name"`
	URL        string  `json:"url"`
	Redirect   *string `json:"redirect_url"`
	Updated    *string `json:"last_updated"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Prefix     string  `json:"storage_prefix"`
	Level      int64   `json:"level"`
	StudyLevel int64   `json:"study_level"`
	Priority   float64 `json:"priority"`
}

const inboxQuery = `WITH files AS MATERIALIZED (
 SELECT f.id,f.local_path,f.name,f.url,f.redirect_url,f.last_updated,n.course_id,c.name course_name,c.webdav_folder,coalesce(s.level,0) level
 FROM app.files f JOIN app.nodes n ON n.id=f.node_id JOIN app.courses c ON c.id=n.course_id
 LEFT JOIN app.file_study s ON s.course_id=n.course_id AND s.file_path=f.local_path
 WHERE f.local_path IS NOT NULL AND ($1::bigint IS NULL OR c.id=$1)
 AND (c.hidden=0 OR ($1::bigint IS NOT NULL AND EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1)))
), completion AS (
 SELECT course_id,coalesce(sum(least(4,greatest(0,level))) FILTER(WHERE level<5)::double precision / nullif(4*count(*) FILTER(WHERE level<5),0),0) ratio
 FROM files GROUP BY course_id
)
SELECT f.local_path,f.name,f.url,f.redirect_url,f.last_updated,f.course_id,f.course_name,f.webdav_folder,f.level,
 least(90,greatest(0,coalesce(CASE WHEN pg_input_is_valid(f.last_updated,'timestamptz') THEN extract(epoch FROM ($2::timestamptz-f.last_updated::timestamptz))/86400 END,30)))::double precision*(1-c.ratio) priority
 FROM files f JOIN completion c USING(course_id) WHERE f.level<4
 ORDER BY priority DESC,f.course_id,f.id LIMIT 60`

func readInbox(ctx context.Context, tx pgx.Tx, selected *int64, now time.Time) ([]InboxItem, error) {
	result := []InboxItem{}
	rows, err := tx.Query(ctx, inboxQuery, selected, now)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var item InboxItem
		if err = rows.Scan(
			&item.Path,
			&item.Name,
			&item.URL,
			&item.Redirect,
			&item.Updated,
			&item.CourseID,
			&item.CourseName,
			&item.Prefix,
			&item.Level,
			&item.Priority,
		); err != nil {
			return nil, err
		}
		for _, value := range []*string{&item.Path, &item.Name, &item.URL, item.Redirect, &item.CourseName, &item.Prefix} {
			if value != nil {
				*value = identity.Decode(*value)
			}
		}
		item.StudyLevel = item.Level
		result = append(result, item)
	}
	return result, rows.Err()
}
