package activity

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

type CoursePage struct {
	Timeline []Item `json:"timeline"`
	More     bool   `json:"has_more"`
	Offset   int64  `json:"offset"`
}

func (s Reader) CoursePage(ctx context.Context, course, limit, offset int64) (CoursePage, error) {
	result := CoursePage{Timeline: []Item{}, Offset: offset}
	if limit < 1 || limit > 50 || offset < 0 || offset > 1<<31-1 {
		return result, errors.New("invalid course update page")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = CoursePageTx(ctx, tx, course, limit, offset)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func CoursePageTx(ctx context.Context, tx pgx.Tx, course, limit, offset int64) (CoursePage, error) {
	result := CoursePage{Timeline: []Item{}, Offset: offset}
	if limit < 1 || limit > 50 || offset < 0 || offset > 1<<31-1 {
		return result, errors.New("invalid course update page")
	}
	var err error
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`, course).Scan(&found); err != nil {
		return result, err
	}
	rows, err := tx.Query(ctx, `WITH events AS (
 SELECT id,timestamp stamp,'change' kind FROM app.change_records WHERE course_id=$1
 UNION ALL SELECT id,pub_date stamp,'announcement' kind FROM app.announcements WHERE course_id=$1
), page AS MATERIALIZED (SELECT * FROM events ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT $2 OFFSET $3)
SELECT CASE WHEN p.kind='change' THEN jsonb_build_object('type',p.kind,'id',p.id,'timestamp',p.stamp,'course_id',$1::bigint,'change_no',r.change_no,'message',r.message)
 ELSE jsonb_build_object('type',p.kind,'id',p.id,'timestamp',p.stamp,'course_id',$1::bigint,'title',a.title,'link',a.link,'description',a.description) END
 FROM page p LEFT JOIN app.change_records r ON p.kind='change' AND r.id=p.id
 LEFT JOIN app.announcements a ON p.kind='announcement' AND a.id=p.id ORDER BY p.stamp DESC NULLS LAST,p.kind,p.id DESC`, course, limit+1, offset)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		if int64(len(result.Timeline)) == limit {
			result.More = true
			break
		}
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return result, err
		}
		var item Item
		if err = json.Unmarshal(raw, &item); err != nil {
			return result, err
		}
		item.decode()
		result.Timeline = append(result.Timeline, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	return result, nil
}
