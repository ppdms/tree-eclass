package database

import "context"

// DiscordExportExpectation carries the in-memory export configuration a
// caller verified before helper I/O. Token is the stored-encoded value;
// callers encode via identity.Encode, adapters compare verbatim.
type DiscordExportExpectation struct {
	Token    string
	Interval int64
	Threads  string
	Media    bool
	Parallel int64
}

// SettingsPreferences is the stored preferences row. Semester bounds are
// raw text; BasePath stays stored-encoded and callers decode it.
type SettingsPreferences struct {
	CheckInterval  int64
	Downloads      int64
	Timeout        int64
	Retries        int64
	Notifications  bool
	NotifyErrors   bool
	DepartmentFeed bool
	UndergradFeed  bool
	RectorFeed     bool
	SemesterStart  *string
	SemesterEnd    *string
	BasePath       string
}

// SettingsCredentials is the stored credential pair. Both fields stay
// stored-encoded exactly as persisted; callers decode them.
type SettingsCredentials struct {
	Username string
	Password string
}

// SettingsDiscord is the stored exporter row. Token stays stored-encoded
// exactly as persisted; callers decode it.
type SettingsDiscord struct {
	Enabled  bool
	Token    string
	Interval int64
	Threads  string
	Media    bool
	Parallel int64
}

// SettingsDiscordChannel pairs one root channel with its mapped course.
// Name stays stored-encoded exactly as persisted; CourseID is nil when
// the root has no mapping.
type SettingsDiscordChannel struct {
	RootID   string
	Name     string
	CourseID *int64
}

// SettingsPlanner is the stored planner row. WeeklyJSON and BlackoutsJSON
// are raw JSON text; the domain owns parsing and defaults.
type SettingsPlanner struct {
	DailyBlocks   int64
	BlockMinutes  int64
	WeeklyJSON    string
	BlackoutsJSON string
	MaxCourses    int64
}

// SettingsExamPlan is one stored exam commitment joined to its course.
// CourseName, ShortName and Notes stay stored-encoded; ExamAt is raw text.
// SaveExamPlan ignores CourseName and reads the encoded ShortName/Notes.
type SettingsExamPlan struct {
	CourseID    int64
	CourseName  string
	ExamAt      *string
	Remaining   int64
	Importance  float64
	MaxBlocks   int64
	Enabled     bool
	ShortName   *string
	Commitment  string
	TargetGrade float64
	Notes       *string
}

// SettingsCheckStatus is the stored course-check progress row. CourseName
// and LastError stay stored-encoded; CourseID is nil when no course is
// selected or the selected course is hidden.
type SettingsCheckStatus struct {
	IsChecking   bool
	StartedAt    *string
	CourseID     *int64
	CourseName   *string
	LastCheckAt  *string
	LastResult   *string
	LastError    *string
	FilesAdded   *int64
	FilesChanged *int64
}

// SettingsSyncStatus is one stored worker heartbeat. LastError and
// LastMessage stay stored-encoded exactly as persisted.
type SettingsSyncStatus struct {
	Job         string
	LastRunAt   *string
	LastResult  *string
	LastError   *string
	LastMessage *string
}

// SettingsExportRow is one normalized learner-export row: keys are the
// allowlisted column names, values are nil (NULL), bool, json.Number or
// stored-encoded string. Callers decode strings via identity.Decode.
type SettingsExportRow map[string]any

// Settings is the typed port for durable configuration: AI selection,
// preferences, credentials, webhook, Discord exporter, planner, exam
// commitments, check/sync status and the learner export. Reads use the
// caller's snapshot; writes bind to the caller's transaction with no
// implicit commits. Missing singleton rows report ErrNoRows and the
// domain applies defaults; stored-encoded text is never decoded here.
type Settings interface {
	// RawAISettings returns the stored AI JSON bytes. It reports
	// ErrNoRows when no row exists; the domain defaults and normalizes.
	RawAISettings(ctx context.Context) ([]byte, error)
	// SaveAISettings upserts the domain-marshalled AI JSON.
	SaveAISettings(ctx context.Context, raw []byte) error
	// LoadPreferences returns the stored row or ErrNoRows when absent.
	LoadPreferences(ctx context.Context) (SettingsPreferences, error)
	// SavePreferences upserts the full row; BasePath is already encoded.
	SavePreferences(ctx context.Context, prefs SettingsPreferences) error
	// LoadCredentials returns the stored pair or ErrNoRows when absent.
	LoadCredentials(ctx context.Context) (SettingsCredentials, error)
	// SaveCredentials upserts the encoded pair.
	SaveCredentials(ctx context.Context, creds SettingsCredentials) error
	// ClearSessionCookie drops the saved session cookie so it cannot
	// authenticate a previous credential pair.
	ClearSessionCookie(ctx context.Context) error
	// LoadWebhook returns the stored-encoded URL or ErrNoRows.
	LoadWebhook(ctx context.Context) (string, error)
	// SaveWebhook upserts the already-encoded URL.
	SaveWebhook(ctx context.Context, encoded string) error
	// LoadDiscordSettings returns the stored row or ErrNoRows.
	LoadDiscordSettings(ctx context.Context) (SettingsDiscord, error)
	// SaveDiscordSettings upserts the full row; Token is already encoded.
	SaveDiscordSettings(ctx context.Context, settings SettingsDiscord) error
	// VerifyDiscordExport holds the export-settings lock on the caller's
	// transaction and compares the stored row verbatim. A missing row
	// reports ErrNoRows; a mismatch reports that the Discord settings
	// changed during export.
	VerifyDiscordExport(ctx context.Context, expected DiscordExportExpectation) error
	// ListDiscordChannels returns every root channel with its mapping,
	// including unmapped roots and mappings whose root is undiscovered,
	// ordered by lower(name) then root id.
	ListDiscordChannels(ctx context.Context) ([]SettingsDiscordChannel, error)
	// ListDiscordMappingRoots returns every known root id from roots and
	// mappings for form validation.
	ListDiscordMappingRoots(ctx context.Context) ([]string, error)
	// ReplaceDiscordMapping deletes every mapping and inserts the given
	// pairs atomically on the caller's transaction.
	ReplaceDiscordMapping(ctx context.Context, mapping map[string]int64) error
	// LoadPlannerSettings returns the stored row or ErrNoRows.
	LoadPlannerSettings(ctx context.Context) (SettingsPlanner, error)
	// SavePlannerSettings upserts the full row.
	SavePlannerSettings(ctx context.Context, planner SettingsPlanner) error
	// LockCoursesForPlanner holds the course-scope guard for a planner
	// save so a concurrent hide/delete cannot change the form's scope.
	// It requires a transaction-bound handle.
	LockCoursesForPlanner(ctx context.Context) error
	// ListExamPlans returns commitments for visible courses and enabled
	// hidden commitments ordered by sort order then id.
	ListExamPlans(ctx context.Context) ([]SettingsExamPlan, error)
	// GetExamPlan returns one course commitment or ErrNoRows.
	GetExamPlan(ctx context.Context, course int64) (SettingsExamPlan, error)
	// SaveExamPlan upserts one commitment and its course short name
	// atomically on the caller's transaction.
	SaveExamPlan(ctx context.Context, plan SettingsExamPlan) error
	// LoadCheckStatus returns the stored progress or ErrNoRows.
	LoadCheckStatus(ctx context.Context) (SettingsCheckStatus, error)
	// ListSyncStatus returns every stored worker heartbeat.
	ListSyncStatus(ctx context.Context) ([]SettingsSyncStatus, error)
	// SchemaVersion returns the applied migration ledger maximum.
	SchemaVersion(ctx context.Context) (int64, error)
	// LockSettingsSection serializes one settings section
	// (credentials, webhook, preferences, discord-export, planner) on
	// the caller's transaction. It requires a transaction-bound handle.
	LockSettingsSection(ctx context.Context, section string) error
	// ExportRows streams one allowlisted learner-export projection in
	// export order without loading the table. Unknown tables report an
	// error; the caller closes the iterator.
	ExportRows(ctx context.Context, table string) (Iterator[SettingsExportRow], error)
}
