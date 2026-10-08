package database

import "context"

// AddCourseParams inserts one course. Name is already encoded by the caller.
type AddCourseParams struct {
	ID           int64   `json:"id"`
	Name         string  `json:"name"`
	WebdavFolder string  `json:"webdav_folder"`
	ShortName    *string `json:"short_name"`
}

// RenameCourseParams renames one course. Name is already encoded by the caller.
type RenameCourseParams struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// HideCourseParams sets the hidden flag (0 visible, 1 hidden) on one course.
type HideCourseParams struct {
	ID     int64 `json:"id"`
	Hidden int64 `json:"hidden"`
}

// OrderCourseParams sets the shelf position of one visible course.
// SortOrder nil clears the position.
type OrderCourseParams struct {
	ID        int64  `json:"id"`
	SortOrder *int64 `json:"sort_order"`
}

// TreeNode is one stored catalog directory. Name and LocalPath stay encoded
// exactly as stored; callers decode via identity.Decode.
type TreeNode struct {
	ID        int64  `json:"id"`
	ParentID  *int64 `json:"parent_id"`
	Name      string `json:"name"`
	URL       string `json:"url"`
	LocalPath string `json:"local_path"`
}

// TreeFile is one stored catalog file. Name and LocalPath stay encoded
// exactly as stored; callers decode via identity.Decode.
type TreeFile struct {
	NodeID      int64   `json:"node_id"`
	URL         string  `json:"url"`
	Name        string  `json:"name"`
	MD5Hash     *string `json:"md5_hash"`
	Etag        *string `json:"etag"`
	LastUpdated *string `json:"last_updated"`
	LocalPath   *string `json:"local_path"`
	RedirectURL *string `json:"redirect_url"`
}

// CourseCoverageRow is the legacy course-coverage export row. PayloadJSON
// stays encoded exactly as stored.
type CourseCoverageRow struct {
	CourseID    int64  `json:"course_id"`
	PayloadJSON string `json:"payload_json"`
}

// RecentMaterialsRow is the legacy recent-materials export row. PayloadJSON
// stays encoded exactly as stored.
type RecentMaterialsRow struct {
	CourseID    int64  `json:"course_id"`
	PayloadJSON string `json:"payload_json"`
}

// StudyLevelRow is one stored learner study level. FilePath stays encoded
// exactly as stored; callers decode via identity.Decode.
type StudyLevelRow struct {
	CourseID int64  `json:"course_id"`
	FilePath string `json:"file_path"`
	Level    int64  `json:"level"`
}

// SetStudyLevelParams upserts one learner study level. FilePath is already
// encoded by the caller; LastUpdated nil stamps the backend clock.
type SetStudyLevelParams struct {
	CourseID    int64   `json:"course_id"`
	FilePath    string  `json:"file_path"`
	Level       int64   `json:"level"`
	LastUpdated *string `json:"last_updated"`
}

// CourseStudyLevel is one stored study level for a single course. FilePath
// stays encoded exactly as stored.
type CourseStudyLevel struct {
	FilePath string `json:"file_path"`
	Level    int64  `json:"level"`
}

// StudyDistributionBucket counts currently registered files at one level.
type StudyDistributionBucket struct {
	Level int64 `json:"level"`
	Count int64 `json:"count"`
}

// FolderPreference is one stored folder collapsed/expanded preference.
// FolderKey stays encoded exactly as stored.
type FolderPreference struct {
	FolderKey string `json:"folder_key"`
	Collapsed int64  `json:"collapsed"`
}

// FileVersionChange is one distinct stored file-version change. FilePath
// stays encoded exactly as stored.
type FileVersionChange struct {
	FilePath   string `json:"file_path"`
	ChangeType string `json:"change_type"`
}

// ShelfRow is one visible course with its cached coverage payload, recent
// materials, generation stamp and staleness flag.
type ShelfRow struct {
	Course     AppCourse `json:"course"`
	Payload    *string   `json:"payload_json"`
	Recent     []byte    `json:"recent_json"`
	Generated  *string   `json:"generated_at"`
	Stale      bool      `json:"stale"`
	Generation int64     `json:"generation"`
}

// StaleCoverageTarget is the lowest-id course whose cached coverage no
// longer matches its source or learner generation.
type StaleCoverageTarget struct {
	CourseID   int64 `json:"course_id"`
	Generation int64 `json:"generation"`
	Learner    int64 `json:"learner_generation"`
}

// PublishCoverageParams replaces the cached coverage row for one course.
// Payload and Recent are already-marshalled JSON; Generated is RFC3339Nano.
type PublishCoverageParams struct {
	CourseID   int64  `json:"course_id"`
	Payload    string `json:"payload_json"`
	Generated  string `json:"generated_at"`
	Generation int64  `json:"generation"`
	Learner    int64  `json:"learner_generation"`
	Recent     []byte `json:"recent_json"`
}

// RecentMaterialRow is one ready current document for the recent-materials
// projection. CourseName, SourcePath and DisplayName stay encoded exactly
// as stored.
type RecentMaterialRow struct {
	ID         string  `json:"id"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	SourcePath string  `json:"source_path"`
	Display    string  `json:"display_name"`
	Indexed    *string `json:"indexed_at"`
}

// Courses is the typed port for course identity, shelf mutations, catalog
// projections, learner study state and coverage. Existing Course,
// VisibleCourse and LockCourseForWrite methods defined by
// MaterialsObjects keep their names and DTO fields stable.
type Courses interface {
	// Course returns one course by id. It reports ErrNoRows when missing.
	Course(ctx context.Context, id int64) (AppCourse, error)
	// VisibleCourse returns the visible course row on the caller's
	// snapshot without taking row locks, so read-only snapshots can use
	// it. It reports ErrNoRows when the course is missing or hidden.
	VisibleCourse(ctx context.Context, id int64) (AppCourse, error)
	// LockCourseForWrite returns the course row under a write guard for
	// publication. It reports ErrNoRows when the course is missing or
	// hidden.
	LockCourseForWrite(ctx context.Context, id int64) (AppCourse, error)
	// ListCourses returns courses ordered by sort_order then id. Hidden
	// courses are included only when includeHidden is true. Added courses
	// default sort_order 0; explicitly cleared NULL positions order after
	// any value on both backends.
	ListCourses(ctx context.Context, includeHidden bool) ([]AppCourse, error)
	// AddCourse inserts one course.
	AddCourse(ctx context.Context, params AddCourseParams) error
	// RenameCourse renames one course and returns the rows affected.
	RenameCourse(ctx context.Context, params RenameCourseParams) (int64, error)
	// HideCourse sets the hidden flag and returns the rows affected.
	HideCourse(ctx context.Context, params HideCourseParams) (int64, error)
	// OrderCourse sets the shelf position of one course.
	OrderCourse(ctx context.Context, params OrderCourseParams) error
	// LockCoursesForReorder serializes shelf reorder on the caller's
	// transaction. It requires a transaction-bound handle.
	LockCoursesForReorder(ctx context.Context) error
	// ExamPlanEnabled reports whether a course has an enabled exam plan.
	ExamPlanEnabled(ctx context.Context, id int64) (bool, error)
	// TreeNodes returns stored directories for one course ordered by id.
	TreeNodes(ctx context.Context, courseID int64) ([]TreeNode, error)
	// TreeFiles returns stored files for one course ordered by file id.
	TreeFiles(ctx context.Context, courseID int64) ([]TreeFile, error)
	// CourseCoverage exports cached coverage payloads ordered by course.
	CourseCoverage(ctx context.Context) ([]CourseCoverageRow, error)
	// RecentMaterials exports recent-materials payloads in ordinal order.
	RecentMaterials(ctx context.Context) ([]RecentMaterialsRow, error)
	// StudyLevels exports every stored study level ordered by course, path.
	StudyLevels(ctx context.Context) ([]StudyLevelRow, error)
	// SetStudyLevel upserts one study level, stamping the backend clock
	// when LastUpdated is nil.
	SetStudyLevel(ctx context.Context, params SetStudyLevelParams) error
	// NodeForFolder reports whether a directory matches a folder key by
	// URL or path. It reports ErrNoRows when no node matches.
	NodeForFolder(ctx context.Context, courseID int64, key string) error
	// SetFolderCollapsed upserts one folder preference, stamping the
	// backend clock on conflict.
	SetFolderCollapsed(ctx context.Context, courseID int64, key string, collapsed int64) error
	// StudyLevelsForCourse returns stored study levels for one course.
	StudyLevelsForCourse(ctx context.Context, courseID int64) ([]CourseStudyLevel, error)
	// StudyDistribution counts currently registered files by level.
	StudyDistribution(ctx context.Context, courseID int64) ([]StudyDistributionBucket, error)
	// CountReadyDocuments counts current ready documents for one course.
	CountReadyDocuments(ctx context.Context, courseID int64) (int64, error)
	// RecentMaterialsForCourse returns up to six ready current documents
	// ordered by indexed stamp then id, newest first.
	RecentMaterialsForCourse(ctx context.Context, courseID int64) ([]RecentMaterialRow, error)
	// FolderPreferences returns stored folder preferences ordered by key.
	FolderPreferences(ctx context.Context, courseID int64) ([]FolderPreference, error)
	// FileVersionChanges returns distinct modified/deleted file-version
	// changes ordered by path.
	FileVersionChanges(ctx context.Context, courseID int64) ([]FileVersionChange, error)
	// ShelfRows returns one row per visible course with cached coverage.
	ShelfRows(ctx context.Context) ([]ShelfRow, error)
	// StaleCoverageTarget returns the lowest-id course needing a coverage
	// rebuild. It reports ErrNoRows when every course is current.
	StaleCoverageTarget(ctx context.Context) (StaleCoverageTarget, error)
	// PublishCoverage replaces the cached coverage row for one course.
	PublishCoverage(ctx context.Context, params PublishCoverageParams) error
	// TryLockCourseForMutation takes the per-course sync/mutation lock.
	// It reports false when another operation holds it. It requires a
	// transaction-bound handle.
	TryLockCourseForMutation(ctx context.Context, courseID int64) (bool, error)
	// ClaimCourseMutation records one destructive intent. It reports true
	// when the intent was newly recorded, false on replay.
	ClaimCourseMutation(ctx context.Context, action string, courseID int64, key string) (bool, error)
	// LockCourseForMutation locks the course row for a destructive
	// mutation. It reports ErrNoRows when the course is missing.
	LockCourseForMutation(ctx context.Context, courseID int64) error
	// ResetCourse removes catalog/history/version rows for one course and
	// retires its eClass documents. It requires a writer transaction.
	ResetCourse(ctx context.Context, courseID int64) error
	// DeleteCourse removes a course and its evidence, queued work and
	// projections. It requires a writer transaction.
	DeleteCourse(ctx context.Context, courseID int64) error
}
