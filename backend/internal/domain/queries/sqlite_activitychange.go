package queries

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
)

// SQLite activity page: change-branch detail loaders.
func (q *SQLiteQueries) sqliteActivityChanges(
	ctx context.Context, changeIDs []int64,
) (map[int64]sqliteChangeDetail, error) {
	changes := map[int64]sqliteChangeDetail{}
	if len(changeIDs) == 0 {
		return changes, nil
	}
	crows, err := q.db.Query(ctx, `SELECT r.id, r.change_no, r.message, c.name, c.short_name
FROM change_records r JOIN courses c ON c.id = r.course_id WHERE r.id IN (`+sqliteActivityIntList(changeIDs)+`)`)
	if err != nil {
		return nil, err
	}
	defer crows.Close()
	for crows.Next() {
		var id int64
		var d sqliteChangeDetail
		var message, shortName sql.NullString
		if err := crows.Scan(&id, &d.changeNo, &message, &d.course, &shortName); err != nil {
			return nil, err
		}
		d.message = activityNullString(message)
		d.shortName = activityNullString(shortName)
		changes[id] = d
	}
	if err := crows.Err(); err != nil {
		return nil, err
	}
	return changes, nil
}

func (q *SQLiteQueries) sqliteActivityChangeItems(
	ctx context.Context, changeIDs []int64,
) (map[int64][]sqliteActivityChangeItem, error) {
	itemsByChange := map[int64][]sqliteActivityChangeItem{}
	for _, id := range changeIDs {
		itemsByChange[id] = []sqliteActivityChangeItem{}
	}
	if len(changeIDs) == 0 {
		return itemsByChange, nil
	}
	irows, err := q.db.Query(ctx, `SELECT change_record_id, change_type, file_path, display_name, redirect_url, diff_webdav_path
FROM change_record_items WHERE change_record_id IN (`+sqliteActivityIntList(changeIDs)+`) ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer irows.Close()
	for irows.Next() {
		var rid int64
		var it sqliteActivityChangeItem
		var display, redirect, diff sql.NullString
		if err := irows.Scan(&rid, &it.ChangeType, &it.FilePath, &display, &redirect, &diff); err != nil {
			return nil, err
		}
		it.DisplayName = activityNullString(display)
		it.RedirectURL = activityNullString(redirect)
		it.DiffWebdavPath = activityNullString(diff)
		itemsByChange[rid] = append(itemsByChange[rid], it)
	}
	if err := irows.Err(); err != nil {
		return nil, err
	}
	return itemsByChange, nil
}

// sqliteActivityIntList renders database-sourced ids for an IN clause.
func sqliteActivityIntList(ids []int64) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(id, 10))
	}
	return b.String()
}
