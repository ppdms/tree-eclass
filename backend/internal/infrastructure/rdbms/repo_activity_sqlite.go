package rdbms

import (
	"context"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
)

// Activity event pages on SQLite. Native schema, placeholders and ordering
// throughout; the initcap mirror for global feed names is shared Go code.
type sqliteActivity struct{ db nativeDBTX }

func sqliteActivityIntList(ids []int64) string {
	var out strings.Builder
	for i, id := range ids {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteString(strconv.FormatInt(id, 10))
	}
	return out.String()
}

func (a sqliteActivity) pageEvents(ctx context.Context, limit, offset int64) ([]activityEventKey, error) {
	rows, err := a.db.Query(ctx, `WITH events AS (`+
		` SELECT r.timestamp AS stamp,r.id,'change' AS kind,r.course_id`+
		` FROM change_records r JOIN courses c ON c.id=r.course_id WHERE c.hidden=0`+
		` UNION ALL SELECT a.pub_date,a.id,'announcement',a.course_id`+
		` FROM announcements a JOIN courses c ON c.id=a.course_id WHERE c.hidden=0`+
		` UNION ALL SELECT pub_date,id,'global',NULL FROM global_announcements`+
		`) SELECT stamp,id,kind,course_id FROM events`+
		` ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []activityEventKey{}
	for rows.Next() {
		var event activityEventKey
		if err := rows.Scan(&event.stamp, &event.id, &event.kind, &event.courseID); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (a sqliteActivity) changeDetails(ctx context.Context, ids []int64) (map[int64]activityChangeDetail, error) {
	out := map[int64]activityChangeDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	// Ids come from the page query above, never from caller input.
	rows, err := a.db.Query(ctx, `SELECT r.id,r.change_no,r.message,c.name,c.short_name`+
		` FROM change_records r JOIN courses c ON c.id=r.course_id`+
		` WHERE r.id IN (`+sqliteActivityIntList(ids)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var detail activityChangeDetail
		if err := rows.Scan(&id, &detail.changeNo, &detail.message, &detail.course, &detail.shortName); err != nil {
			return nil, err
		}
		out[id] = detail
	}
	return out, rows.Err()
}

func (a sqliteActivity) changeItems(ctx context.Context, ids []int64) (map[int64][]database.ActivityChangeItem, error) {
	out := map[int64][]database.ActivityChangeItem{}
	for _, id := range ids {
		out[id] = []database.ActivityChangeItem{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT change_record_id,change_type,file_path,display_name,`+
		`redirect_url,diff_webdav_path FROM change_record_items`+
		` WHERE change_record_id IN (`+sqliteActivityIntList(ids)+`) ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var record int64
		var item database.ActivityChangeItem
		if err := rows.Scan(&record, &item.ChangeType, &item.FilePath,
			&item.Display, &item.Redirect, &item.Diff); err != nil {
			return nil, err
		}
		out[record] = append(out[record], item)
	}
	return out, rows.Err()
}

func (a sqliteActivity) announcementDetails(
	ctx context.Context, ids []int64) (map[int64]activityAnnouncementDetail, error) {
	out := map[int64]activityAnnouncementDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT a.id,a.title,a.link,a.description,c.name,c.short_name`+
		` FROM announcements a LEFT JOIN courses c ON c.id=a.course_id`+
		` WHERE a.id IN (`+sqliteActivityIntList(ids)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var detail activityAnnouncementDetail
		var course *string
		if err := rows.Scan(&id, &detail.title, &detail.link, &detail.desc, &course, &detail.shortName); err != nil {
			return nil, err
		}
		if course != nil {
			detail.course = *course
		}
		out[id] = detail
	}
	return out, rows.Err()
}

func (a sqliteActivity) globalDetails(ctx context.Context, ids []int64) (map[int64]activityGlobalDetail, error) {
	out := map[int64]activityGlobalDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT id,feed_key,title,link,description FROM global_announcements`+
		` WHERE id IN (`+sqliteActivityIntList(ids)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var detail activityGlobalDetail
		var feed *string
		if err := rows.Scan(&id, &feed, &detail.title, &detail.link, &detail.desc); err != nil {
			return nil, err
		}
		if feed != nil {
			detail.feedKey = *feed
		}
		out[id] = detail
	}
	return out, rows.Err()
}

func (a sqliteActivity) Page(ctx context.Context, limit, offset int64) ([]database.ActivityEvent, error) {
	events, err := a.pageEvents(ctx, limit, offset)
	if err != nil {
		return nil, err
	}
	changes, announces, globals := splitActivityIDs(events)
	var details activityDetails
	if details.changes, err = a.changeDetails(ctx, changes); err != nil {
		return nil, err
	}
	if details.items, err = a.changeItems(ctx, changes); err != nil {
		return nil, err
	}
	if details.announce, err = a.announcementDetails(ctx, announces); err != nil {
		return nil, err
	}
	if details.globals, err = a.globalDetails(ctx, globals); err != nil {
		return nil, err
	}
	return assembleActivityEvents(events, details), nil
}

func (a sqliteActivity) CoursePage(
	ctx context.Context, course, limit, offset int64) ([]database.ActivityCourseEvent, error) {
	var found int64
	if err := a.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=? AND hidden=0`,
		course).Scan(&found); err != nil {
		return nil, err
	}
	rows, err := a.db.Query(ctx, `WITH events AS (`+
		` SELECT id,timestamp stamp,'change' kind FROM change_records WHERE course_id=?`+
		` UNION ALL SELECT id,pub_date stamp,'announcement' kind FROM announcements WHERE course_id=?`+
		`), page AS (SELECT * FROM events ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT ? OFFSET ?)`+
		` SELECT p.kind,p.id,p.stamp,r.change_no,r.message,a.title,a.link,a.description`+
		` FROM page p LEFT JOIN change_records r ON p.kind='change' AND r.id=p.id`+
		` LEFT JOIN announcements a ON p.kind='announcement' AND a.id=p.id`+
		` ORDER BY p.stamp DESC NULLS LAST,p.kind,p.id DESC`, course, course, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ActivityCourseEvent{}
	for rows.Next() {
		var item database.ActivityCourseEvent
		var changeNo *string
		if err := rows.Scan(&item.Kind, &item.ID, &item.Timestamp, &changeNo,
			&item.Message, &item.Title, &item.Link, &item.Desc); err != nil {
			return nil, err
		}
		if changeNo != nil {
			item.ChangeNo = *changeNo
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
