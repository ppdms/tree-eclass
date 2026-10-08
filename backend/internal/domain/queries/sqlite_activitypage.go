package queries

import (
	"context"
	"encoding/json"
	"strconv"
)

// SQLite activity page assembly. ActivityPage keeps the same UNION ALL
// event shape and ordering as the postgres query; detail bodies are loaded
// per branch and the JSON payload assembled in Go with the same keys.

const sqliteActivityPageSQL = `-- name: ActivityPage :many
WITH events AS (
    SELECT r.timestamp AS stamp, r.id, 'change' AS kind, r.course_id
      FROM change_records r JOIN courses c ON c.id = r.course_id WHERE c.hidden = 0
    UNION ALL
    SELECT a.pub_date, a.id, 'announcement', a.course_id
      FROM announcements a JOIN courses c ON c.id = a.course_id WHERE c.hidden = 0
    UNION ALL SELECT pub_date, id, 'global', NULL FROM global_announcements
)
SELECT stamp, id, kind, course_id FROM events ORDER BY stamp DESC NULLS LAST, kind, id DESC LIMIT $1 OFFSET $2
`

type sqliteActivityBase struct {
	Type            string  `json:"type"`
	ID              any     `json:"id"`
	Timestamp       *string `json:"timestamp"`
	SortKey         string  `json:"sort_key"`
	CourseID        *int64  `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
}

type sqliteActivityChangeItem struct {
	ChangeType     string  `json:"change_type"`
	FilePath       string  `json:"file_path"`
	DisplayName    *string `json:"display_name"`
	RedirectURL    *string `json:"redirect_url"`
	DiffWebdavPath *string `json:"diff_webdav_path"`
}

type sqliteActivityChange struct {
	sqliteActivityBase
	ChangeNo string                     `json:"change_no"`
	Message  *string                    `json:"message"`
	Changes  []sqliteActivityChangeItem `json:"changes"`
}

type sqliteActivityAnnouncement struct {
	sqliteActivityBase
	Title       *string `json:"title"`
	Link        *string `json:"link"`
	Description *string `json:"description"`
}

type sqliteActivityEvent struct {
	stamp    *string
	id       int64
	kind     string
	courseID *int64
}

type sqliteChangeDetail struct {
	changeNo  string
	message   *string
	course    string
	shortName *string
}

type sqliteAnnouncementDetail struct {
	title     *string
	link      *string
	desc      *string
	course    string
	shortName *string
}

type sqliteGlobalDetail struct {
	feedKey string
	title   *string
	link    *string
	desc    *string
}

type sqliteActivityDetails struct {
	changes  map[int64]sqliteChangeDetail
	items    map[int64][]sqliteActivityChangeItem
	announce map[int64]sqliteAnnouncementDetail
	globals  map[int64]sqliteGlobalDetail
}

func (q *SQLiteQueries) sqliteActivityDetails(
	ctx context.Context, events []sqliteActivityEvent,
) (sqliteActivityDetails, error) {
	var out sqliteActivityDetails
	var changeIDs, announceIDs, globalIDs []int64
	for _, e := range events {
		switch e.kind {
		case "change":
			changeIDs = append(changeIDs, e.id)
		case "announcement":
			announceIDs = append(announceIDs, e.id)
		case "global":
			globalIDs = append(globalIDs, e.id)
		}
	}
	var err error
	if out.changes, err = q.sqliteActivityChanges(ctx, changeIDs); err != nil {
		return sqliteActivityDetails{}, err
	}
	if out.items, err = q.sqliteActivityChangeItems(ctx, changeIDs); err != nil {
		return sqliteActivityDetails{}, err
	}
	if out.announce, err = q.sqliteActivityAnnounces(ctx, announceIDs); err != nil {
		return sqliteActivityDetails{}, err
	}
	if out.globals, err = q.sqliteActivityGlobals(ctx, globalIDs); err != nil {
		return sqliteActivityDetails{}, err
	}
	return out, nil
}

func assembleActivityPayloads(events []sqliteActivityEvent, d sqliteActivityDetails) ([][]byte, error) {
	payloads := [][]byte{}
	for _, e := range events {
		sortKey := ""
		if e.stamp != nil {
			sortKey = *e.stamp
		}
		var payload []byte
		var err error
		switch e.kind {
		case "change":
			payload, err = marshalActivityChange(e, sortKey, d)
		case "announcement":
			payload, err = marshalActivityAnnouncement(e, sortKey, d)
		case "global":
			payload, err = marshalActivityGlobal(e, sortKey, d)
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		if payload == nil {
			continue
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
}

func marshalActivityChange(e sqliteActivityEvent, sortKey string, d sqliteActivityDetails) ([]byte, error) {
	c, ok := d.changes[e.id]
	if !ok {
		return nil, nil
	}
	return json.Marshal(sqliteActivityChange{
		sqliteActivityBase: sqliteActivityBase{
			Type: "change", ID: e.id, Timestamp: e.stamp,
			SortKey: sortKey, CourseID: e.courseID,
			CourseName: c.course, CourseShortName: c.shortName,
		},
		ChangeNo: c.changeNo, Message: c.message,
		Changes: d.items[e.id],
	})
}

func marshalActivityAnnouncement(e sqliteActivityEvent, sortKey string, d sqliteActivityDetails) ([]byte, error) {
	a, ok := d.announce[e.id]
	if !ok {
		return nil, nil
	}
	return json.Marshal(sqliteActivityAnnouncement{
		sqliteActivityBase: sqliteActivityBase{
			Type: "announcement", ID: e.id, Timestamp: e.stamp,
			SortKey: sortKey, CourseID: e.courseID,
			CourseName: a.course, CourseShortName: a.shortName,
		},
		Title: a.title, Link: a.link, Description: a.desc,
	})
}

func marshalActivityGlobal(e sqliteActivityEvent, sortKey string, d sqliteActivityDetails) ([]byte, error) {
	g, ok := d.globals[e.id]
	if !ok {
		return nil, nil
	}
	return json.Marshal(sqliteActivityAnnouncement{
		sqliteActivityBase: sqliteActivityBase{
			Type: "announcement", ID: "global_" + strconv.FormatInt(e.id, 10),
			Timestamp: e.stamp, SortKey: sortKey, CourseID: nil,
			CourseName: sqliteGlobalCourseName(g.feedKey), CourseShortName: nil,
		},
		Title: g.title, Link: g.link, Description: g.desc,
	})
}

func (q *SQLiteQueries) ActivityPage(ctx context.Context, arg ActivityPageParams) ([][]byte, error) {
	events, err := q.sqliteActivityPageQuery(ctx, arg)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return [][]byte{}, nil
	}
	details, err := q.sqliteActivityDetails(ctx, events)
	if err != nil {
		return nil, err
	}
	return assembleActivityPayloads(events, details)
}
