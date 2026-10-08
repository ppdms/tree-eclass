package database

import "context"

// ActivityChangeItem is one stored change-record item. FilePath stays
// encoded exactly as stored; callers decode via identity.Decode.
type ActivityChangeItem struct {
	ChangeType string  `json:"change_type"`
	FilePath   string  `json:"file_path"`
	Display    *string `json:"display_name"`
	Redirect   *string `json:"redirect_url"`
	Diff       *string `json:"diff_webdav_path"`
}

// ActivityEvent is one typed activity event. Kind is change, announcement
// or global. ID is the numeric row id; global events format it as
// "global_<id>". Timestamp and SortKey stay encoded exactly as stored.
// CourseName, ShortName, Message, Title, Link and Description stay encoded
// exactly as stored; callers decode via identity.Decode. Change items are
// attached for change events only.
type ActivityEvent struct {
	Kind       string               `json:"kind"`
	Type       string               `json:"type"`
	ID         int64                `json:"id"`
	Timestamp  *string              `json:"timestamp"`
	SortKey    string               `json:"sort_key"`
	CourseID   *int64               `json:"course_id"`
	CourseName string               `json:"course_name"`
	ShortName  *string              `json:"course_short_name"`
	ChangeNo   string               `json:"change_no"`
	Message    *string              `json:"message"`
	Changes    []ActivityChangeItem `json:"changes"`
	Title      *string              `json:"title"`
	Link       *string              `json:"link"`
	Desc       *string              `json:"description"`
}

// ActivityCourseEvent is one typed course-scoped event. Kind is change or
// announcement. Fields stay encoded exactly as stored.
type ActivityCourseEvent struct {
	Kind      string  `json:"kind"`
	ID        int64   `json:"id"`
	Timestamp *string `json:"timestamp"`
	ChangeNo  string  `json:"change_no"`
	Message   *string `json:"message"`
	Title     *string `json:"title"`
	Link      *string `json:"link"`
	Desc      *string `json:"description"`
}

// Activity is the typed port for bounded activity event pages.
type Activity interface {
	// Page returns up to limit+1 events ordered by stamp descending with
	// NULL stamps last, then kind, then id descending. Callers request
	// one extra row to detect a following page.
	Page(ctx context.Context, limit, offset int64) ([]ActivityEvent, error)
	// CoursePage returns up to limit+1 events for one visible course in
	// the same order. It reports ErrNoRows when the course is missing or
	// hidden.
	CoursePage(ctx context.Context, course, limit, offset int64) ([]ActivityCourseEvent, error)
}
