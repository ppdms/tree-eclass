package database

import "context"

// SyncDirectoryRow is one stored tree directory. Path/Parent/Name stay
// encoded exactly as stored; callers decode via identity.Decode.
type SyncDirectoryRow struct {
	ID       int64
	CourseID int64
	ParentID *int64
	Name     string
	URL      string
	Path     string
}

// SyncFileRow is one stored tree file joined to its directory path and its
// optional immutable object. Object is nil for redirect-only rows.
type SyncFileRow struct {
	NodeID   int64
	Parent   string
	URL      string
	Name     string
	Path     string
	MD5      string
	ETag     string
	Redirect string
	Updated  string
	Revision string
	Object   *ObjectReference
}

// SyncDirectoryInput inserts one tree directory. ParentID is nil for the
// crawl root. Name/Path are already encoded by the caller.
type SyncDirectoryInput struct {
	CourseID int64
	ParentID *int64
	Name     string
	URL      string
	Path     string
}

// SyncFileInput inserts one tree file. ObjectID/RevisionID are nil for
// redirect rows; Redirect empty stores SQL NULL via NULLIF semantics.
type SyncFileInput struct {
	NodeID     int64
	URL        string
	Name       string
	MD5        string
	ETag       string
	Redirect   string
	Updated    string
	Path       string
	ObjectID   *string
	RevisionID *string
}

// SyncCourseGuard is the locked course row a sync publish validates before
// replacing the tree. Fields are stored-encoded where text came from names.
type SyncCourseGuard struct {
	Name         string
	WebdavFolder string
	Hidden       int64
}

// SyncChangeRecord is one stored change record with its stored timestamp.
type SyncChangeRecord struct {
	ID        int64
	CourseID  int64
	Number    string
	Timestamp string
	Message   *string
	Count     int64
}

// SyncHistoryItem is one stored change-record item.
type SyncHistoryItem struct {
	Type     string
	Path     string
	Name     *string
	Redirect *string
	Diff     *string
}

// SyncChangeRecordItemInput inserts one change-record item. Redirect empty
// stores SQL NULL; Difference/ DiffAlias nil preserves NULL.
type SyncChangeRecordItemInput struct {
	RecordID   int64
	Type       string
	Path       string
	Name       string
	Redirect   string
	Difference *string
	DiffAlias  *string
}

// SyncFileVersionInput archives one previous file revision. StoragePath and
// Revision/Difference/DiffAlias nil preserve NULL; Redirect empty is NULL.
type SyncFileVersionInput struct {
	CourseID    int64
	Path        string
	StoragePath *string
	Type        string
	Name        string
	Redirect    string
	Revision    *string
	Difference  *string
	DiffAlias   *string
}

// SyncVersion is one stored file version with its stored timestamp.
type SyncVersion struct {
	ID          int64
	CourseID    int64
	Path        string
	StoragePath *string
	Type        string
	Timestamp   string
	Name        *string
	Redirect    *string
	Diff        *string
}

// SyncAnnouncementInput upserts one course announcement. Published nil
// preserves NULL; ID/Title/Description are already encoded by the caller.
type SyncAnnouncementInput struct {
	CourseID    int64
	ID          string
	Title       string
	Link        string
	Description string
	Published   *string
}

// SyncExerciseInput upserts one course exercise. Text fields are already
// encoded by the caller; Link/AssignmentFileURL stay raw URLs.
type SyncExerciseInput struct {
	CourseID           int64
	ID                 string
	Title              string
	Link               string
	Deadline           string
	SubmissionStatus   string
	Grade              string
	WorkType           string
	Description        string
	StartDate          string
	MaxGrade           string
	AssignmentFileName string
	AssignmentFileURL  string
	GradeComments      string
	SubmissionDate     string
}

// SyncExerciseState is the stored notification-relevant exercise snapshot.
type SyncExerciseState struct {
	Grade              string
	GradeComments      string
	AssignmentFileName string
	AssignmentFileURL  string
}

// SyncGlobalAnnouncementInput upserts one university-feed announcement.
type SyncGlobalAnnouncementInput struct {
	FeedKey     string
	ID          string
	Title       string
	Link        string
	Description string
	Published   *string
}

// SyncClaim is the pending/running sync control command a retry inspects.
type SyncClaim struct {
	ID      string
	Status  string
	Payload []byte
	Failure *string
}

// SyncCheckFinish records the terminal check outcome.
type SyncCheckFinish struct {
	At          string
	Status      string
	Error       *string
	FilesAdded  int
	FilesChange int
}

// SyncCheckStart records the admitted check's start marker.
type SyncCheckStart struct {
	At       string
	CourseID *int64
}

// Sync is the typed port for eClass synchronization: course-tree publish,
// change history, feeds, admission claims and check status. Every method on
// a Tx binds to that transaction; crawl reads use the store snapshot.
type Sync interface {
	// TryLockCourse takes the per-course publish lock (extended advisory
	// namespace). It reports false when another publish holds it. SQLite
	// always acquires on a writer transaction via writer admission.
	TryLockCourse(ctx context.Context, courseID int64) (bool, error)
	// LockGlobalFeed serializes one university feed publish on the
	// caller's transaction (extended advisory namespace).
	LockGlobalFeed(ctx context.Context, feed string) error
	// LockCourseForSync locks the course row for a publish and returns
	// the guard fields. It reports ErrNoRows when missing.
	LockCourseForSync(ctx context.Context, courseID int64) (SyncCourseGuard, error)
	// CourseHidden locks the course row and returns its hidden flag. It
	// reports ErrNoRows when the course does not exist.
	CourseHidden(ctx context.Context, courseID int64) (int64, error)
	// CourseName returns the stored (encoded) course name.
	CourseName(ctx context.Context, courseID int64) (string, error)
	// CourseVisible reports whether a course exists and is not hidden.
	CourseVisible(ctx context.Context, courseID int64) (bool, error)
	// ListTreeDirectories returns stored directories ordered by id.
	ListTreeDirectories(ctx context.Context, courseID int64) ([]SyncDirectoryRow, error)
	// ListTreeFiles returns stored files joined to directory paths and
	// objects, ordered by file id.
	ListTreeFiles(ctx context.Context, courseID int64) ([]SyncFileRow, error)
	// DeleteTree removes every stored node (files cascade) for a course.
	DeleteTree(ctx context.Context, courseID int64) error
	// InsertDirectory inserts one tree directory and returns its id.
	InsertDirectory(ctx context.Context, dir SyncDirectoryInput) (int64, error)
	// InsertFile inserts one tree file.
	InsertFile(ctx context.Context, file SyncFileInput) error
	// RetireMissingEclassDocuments marks eClass documents not in current
	// as not current, excluding archive members. Empty current retires
	// every non-member eClass document for the course.
	RetireMissingEclassDocuments(ctx context.Context, courseID int64, current []string) error
	// InsertChangeRecord inserts one change record and returns its id.
	InsertChangeRecord(ctx context.Context, courseID int64, number, message string, count int) (int64, error)
	// InsertChangeHistory appends one change-history row.
	InsertChangeHistory(ctx context.Context, courseID int64, changeType, path string) error
	// InsertChangeRecordItem appends one change-record item.
	InsertChangeRecordItem(ctx context.Context, item SyncChangeRecordItemInput) error
	// InsertFileVersion archives one previous file revision.
	InsertFileVersion(ctx context.Context, version SyncFileVersionInput) error
	// AnnouncementExists reports whether an announcement row exists.
	AnnouncementExists(ctx context.Context, courseID int64, id string) (bool, error)
	// UpsertAnnouncement inserts or refreshes one course announcement,
	// stamping the backend clock on conflict.
	UpsertAnnouncement(ctx context.Context, announcement SyncAnnouncementInput) error
	// ExerciseState returns the stored notification-relevant exercise
	// snapshot. It reports ErrNoRows when the exercise is new.
	ExerciseState(ctx context.Context, courseID int64, id string) (SyncExerciseState, error)
	// UpsertExercise inserts or refreshes one course exercise, stamping
	// the backend clock on conflict.
	UpsertExercise(ctx context.Context, exercise SyncExerciseInput) error
	// GlobalAnnouncementExists reports whether a feed row exists.
	GlobalAnnouncementExists(ctx context.Context, feed, id string) (bool, error)
	// UpsertGlobalAnnouncement inserts or refreshes one feed row.
	UpsertGlobalAnnouncement(ctx context.Context, announcement SyncGlobalAnnouncementInput) error
	// ListVersions returns stored file versions newest first, filtered by
	// change kind and optional exact file and folder-prefix filters.
	ListVersions(ctx context.Context, courseID int64, kind string, file, folder *string) ([]SyncVersion, error)
	// ChangeRecord loads one change record with its decoded-later course
	// name for a visible course. It reports ErrNoRows when missing.
	ChangeRecord(ctx context.Context, courseID int64, number string) (SyncChangeRecord, string, error)
	// ChangeRecordItems lists stored items for one record ordered by id.
	ChangeRecordItems(ctx context.Context, recordID int64) ([]SyncHistoryItem, error)
	// FindSyncClaim returns the pending/running sync command, locking it
	// where the backend supports row locks. ErrNoRows when none.
	FindSyncClaim(ctx context.Context) (SyncClaim, error)
	// ResetSyncClaim re-arms one pending failed claim for immediate retry.
	ResetSyncClaim(ctx context.Context, id string) error
	// MarkCheckStart records the admitted check start marker.
	MarkCheckStart(ctx context.Context, start SyncCheckStart) error
	// MarkCourseChecking marks the in-progress course on check status.
	MarkCourseChecking(ctx context.Context, courseID int64) error
	// FinishCheck records the terminal check outcome and clears progress.
	FinishCheck(ctx context.Context, finish SyncCheckFinish) error
}
