package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// Activity event pages on PostgreSQL. The event CTE keeps the original
// UNION ALL shape and ordering; details load per branch so both backends
// assemble identical typed events without JSON shims.
type postgresActivity struct{ db nativeDBTX }

func (a postgresActivity) pageEvents(ctx context.Context, limit, offset int64) ([]activityEventKey, error) {
	rows, err := a.db.Query(ctx, `WITH events AS (`+
		` SELECT r.timestamp AS stamp,r.id,'change' AS kind,r.course_id`+
		` FROM app.change_records r JOIN app.courses c ON c.id=r.course_id WHERE c.hidden=0`+
		` UNION ALL SELECT a.pub_date,a.id,'announcement',a.course_id`+
		` FROM app.announcements a JOIN app.courses c ON c.id=a.course_id WHERE c.hidden=0`+
		` UNION ALL SELECT pub_date,id,'global',NULL::bigint FROM app.global_announcements`+
		`) SELECT stamp,id,kind,course_id FROM events`+
		` ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT $1 OFFSET $2`, limit, offset)
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

func (a postgresActivity) changeDetails(ctx context.Context, ids []int64) (map[int64]activityChangeDetail, error) {
	out := map[int64]activityChangeDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT r.id,r.change_no,r.message,c.name,c.short_name`+
		` FROM app.change_records r JOIN app.courses c ON c.id=r.course_id WHERE r.id=ANY($1::bigint[])`, ids)
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

func (a postgresActivity) changeItems(
	ctx context.Context, ids []int64) (map[int64][]database.ActivityChangeItem, error) {
	out := map[int64][]database.ActivityChangeItem{}
	for _, id := range ids {
		out[id] = []database.ActivityChangeItem{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT change_record_id,change_type,file_path,display_name,`+
		`redirect_url,diff_webdav_path FROM app.change_record_items`+
		` WHERE change_record_id=ANY($1::bigint[]) ORDER BY id`, ids)
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

func (a postgresActivity) announcementDetails(
	ctx context.Context, ids []int64) (map[int64]activityAnnouncementDetail, error) {
	out := map[int64]activityAnnouncementDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT a.id,a.title,a.link,a.description,c.name,c.short_name`+
		` FROM app.announcements a LEFT JOIN app.courses c ON c.id=a.course_id WHERE a.id=ANY($1::bigint[])`, ids)
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

func (a postgresActivity) globalDetails(ctx context.Context, ids []int64) (map[int64]activityGlobalDetail, error) {
	out := map[int64]activityGlobalDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := a.db.Query(ctx, `SELECT id,feed_key,title,link,description FROM app.global_announcements`+
		` WHERE id=ANY($1::bigint[])`, ids)
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

func (a postgresActivity) Page(ctx context.Context, limit, offset int64) ([]database.ActivityEvent, error) {
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

func (a postgresActivity) CoursePage(
	ctx context.Context, course, limit, offset int64) ([]database.ActivityCourseEvent, error) {
	var found int64
	if err := a.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`,
		course).Scan(&found); err != nil {
		return nil, err
	}
	rows, err := a.db.Query(ctx, `WITH events AS (`+
		` SELECT id,timestamp stamp,'change' kind FROM app.change_records WHERE course_id=$1`+
		` UNION ALL SELECT id,pub_date stamp,'announcement' kind FROM app.announcements WHERE course_id=$1`+
		`), page AS (SELECT * FROM events ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT $2 OFFSET $3)`+
		` SELECT p.kind,p.id,p.stamp,r.change_no,r.message,a.title,a.link,a.description`+
		` FROM page p LEFT JOIN app.change_records r ON p.kind='change' AND r.id=p.id`+
		` LEFT JOIN app.announcements a ON p.kind='announcement' AND a.id=p.id`+
		` ORDER BY p.stamp DESC NULLS LAST,p.kind,p.id DESC`, course, limit, offset)
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
