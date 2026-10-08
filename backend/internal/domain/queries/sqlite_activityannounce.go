package queries

import (
	"context"
	"database/sql"
	"strings"
	"unicode"
)

// SQLite activity page: announcement/global detail loaders and the
// postgres-initcap mirror used for global feed names.

func (q *SQLiteQueries) sqliteActivityAnnounces(
	ctx context.Context, announceIDs []int64,
) (map[int64]sqliteAnnouncementDetail, error) {
	announces := map[int64]sqliteAnnouncementDetail{}
	if len(announceIDs) == 0 {
		return announces, nil
	}
	arows, err := q.db.Query(ctx, `SELECT a.id, a.title, a.link, a.description, c.name, c.short_name
FROM announcements a LEFT JOIN courses c ON c.id = a.course_id WHERE a.id IN (`+sqliteActivityIntList(announceIDs)+`)`)
	if err != nil {
		return nil, err
	}
	defer arows.Close()
	for arows.Next() {
		var id int64
		var d sqliteAnnouncementDetail
		var title, link, desc, course sql.NullString
		var shortName sql.NullString
		if err := arows.Scan(&id, &title, &link, &desc, &course, &shortName); err != nil {
			return nil, err
		}
		d.title = activityNullString(title)
		d.link = activityNullString(link)
		d.desc = activityNullString(desc)
		if course.Valid {
			d.course = course.String
		}
		d.shortName = activityNullString(shortName)
		announces[id] = d
	}
	if err := arows.Err(); err != nil {
		return nil, err
	}
	return announces, nil
}

func (q *SQLiteQueries) sqliteActivityGlobals(
	ctx context.Context, globalIDs []int64,
) (map[int64]sqliteGlobalDetail, error) {
	globals := map[int64]sqliteGlobalDetail{}
	if len(globalIDs) == 0 {
		return globals, nil
	}
	grows, err := q.db.Query(ctx, `SELECT id, feed_key, title, link, description
FROM global_announcements WHERE id IN (`+sqliteActivityIntList(globalIDs)+`)`)
	if err != nil {
		return nil, err
	}
	defer grows.Close()
	for grows.Next() {
		var id int64
		var d sqliteGlobalDetail
		var feedKey sql.NullString
		var title, link, desc sql.NullString
		if err := grows.Scan(&id, &feedKey, &title, &link, &desc); err != nil {
			return nil, err
		}
		if feedKey.Valid {
			d.feedKey = feedKey.String
		}
		d.title = activityNullString(title)
		d.link = activityNullString(link)
		d.desc = activityNullString(desc)
		globals[id] = d
	}
	if err := grows.Err(); err != nil {
		return nil, err
	}
	return globals, nil
}

// sqliteActivityInitcap mirrors postgres initcap: first alphanumeric of
// each word upper-cased, the rest lower-cased; words split on
// non-alphanumerics.
func sqliteActivityInitcap(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	upper := true
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if upper {
				b.WriteRune(unicode.ToUpper(r))
			} else {
				b.WriteRune(unicode.ToLower(r))
			}
			upper = false
		} else {
			b.WriteRune(r)
			upper = true
		}
	}
	return b.String()
}

// sqliteGlobalCourseName mirrors
// initcap(replace(coalesce(feed_key,'Global'),'_',' ')).
func sqliteGlobalCourseName(feedKey string) string {
	if feedKey == "" {
		feedKey = "Global"
	}
	return sqliteActivityInitcap(strings.ReplaceAll(feedKey, "_", " "))
}
