package rdbms

import (
	"strings"
	"unicode"

	"tree-eclass/internal/domain/database"
)

// Shared activity detail shapes and typed event assembly. Both backends load
// per-branch details with native SQL, then assemble identical events here.

type activityChangeDetail struct {
	changeNo  string
	message   *string
	course    string
	shortName *string
}

type activityAnnouncementDetail struct {
	title     *string
	link      *string
	desc      *string
	course    string
	shortName *string
}

type activityGlobalDetail struct {
	feedKey string
	title   *string
	link    *string
	desc    *string
}

type activityEventKey struct {
	stamp    *string
	id       int64
	kind     string
	courseID *int64
}

type activityDetails struct {
	changes  map[int64]activityChangeDetail
	items    map[int64][]database.ActivityChangeItem
	announce map[int64]activityAnnouncementDetail
	globals  map[int64]activityGlobalDetail
}

// globalCourseName mirrors initcap(replace(coalesce(feed_key,'Global'),'_',' '))
// so both backends agree on university-feed course names.
func globalCourseName(feedKey string) string {
	if feedKey == "" {
		feedKey = "Global"
	}
	text := strings.ReplaceAll(feedKey, "_", " ")
	var out strings.Builder
	out.Grow(len(text))
	upper := true
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if upper {
				out.WriteRune(unicode.ToUpper(r))
			} else {
				out.WriteRune(unicode.ToLower(r))
			}
			upper = false
		} else {
			out.WriteRune(r)
			upper = true
		}
	}
	return out.String()
}

func assembleActivityEvents(events []activityEventKey, details activityDetails) []database.ActivityEvent {
	out := []database.ActivityEvent{}
	for _, event := range events {
		item := database.ActivityEvent{Kind: event.kind, ID: event.id, Timestamp: event.stamp,
			CourseID: event.courseID}
		if event.stamp != nil {
			item.SortKey = *event.stamp
		}
		switch event.kind {
		case "change":
			detail, ok := details.changes[event.id]
			if !ok {
				continue
			}
			item.Type = "change"
			item.CourseName, item.ShortName = detail.course, detail.shortName
			item.ChangeNo, item.Message = detail.changeNo, detail.message
			item.Changes = details.items[event.id]
		case "announcement":
			detail, ok := details.announce[event.id]
			if !ok {
				continue
			}
			item.Type = "announcement"
			item.CourseName, item.ShortName = detail.course, detail.shortName
			item.Title, item.Link, item.Desc = detail.title, detail.link, detail.desc
		case "global":
			detail, ok := details.globals[event.id]
			if !ok {
				continue
			}
			item.Type = "announcement"
			item.CourseID = nil
			item.CourseName = globalCourseName(detail.feedKey)
			item.Title, item.Link, item.Desc = detail.title, detail.link, detail.desc
		default:
			continue
		}
		out = append(out, item)
	}
	return out
}

func splitActivityIDs(events []activityEventKey) (changes, announces, globals []int64) {
	for _, event := range events {
		switch event.kind {
		case "change":
			changes = append(changes, event.id)
		case "announcement":
			announces = append(announces, event.id)
		case "global":
			globals = append(globals, event.id)
		}
	}
	return changes, announces, globals
}
