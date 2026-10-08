// Package database defines SQL-free persistence contracts and neutral values.
package database

import "context"

// PracticeQuestionSet is the neutral current practice set row for one unit.
// GeneratedAt is the stored timestamp text exactly as recorded; nil when unset.
type PracticeQuestionSet struct {
	ID        int64   `json:"id"`
	CourseID  int64   `json:"course_id"`
	UnitKey   string  `json:"unit_key"`
	SetHash   string  `json:"set_hash"`
	Revision  string  `json:"blueprint_revision_hash"`
	Status    string  `json:"status"`
	Model     string  `json:"model"`
	Generated *string `json:"generated_at"`
}

// PracticeSetSelector scopes set selection. Revision and AnalysisVersion must
// match exactly; Model matches the configured practice model exactly. Empty
// UnitKey selects every unit; otherwise only that unit.
type PracticeSetSelector struct {
	CourseID        int64  `json:"course_id"`
	Revision        string `json:"blueprint_revision_hash"`
	AnalysisVersion string `json:"analysis_version"`
	Model           string `json:"model"`
	UnitKey         string `json:"unit_key"`
}

// PracticeQuestionRow is one stored question row joined to its set.
// Payload is nil only when the stored payload exceeds the 64 KiB budget.
type PracticeQuestionRow struct {
	SetID    int64   `json:"set_id"`
	Question string  `json:"question_id"`
	CourseID int64   `json:"course_id"`
	UnitKey  string  `json:"unit_key"`
	Key      string  `json:"question_key"`
	Payload  *string `json:"payload_json"`
}

// PracticeAttemptRow is the stored idempotent attempt row with the question
// fingerprints that detect a conflicting retry. Confidence, Seconds, Answer
// and Note are nil when unset; Answer/Note stay encoded exactly as stored.
type PracticeAttemptRow struct {
	ID          int64   `json:"id"`
	CourseID    int64   `json:"course_id"`
	Unit        string  `json:"unit_key"`
	Question    string  `json:"question_id"`
	Outcome     string  `json:"outcome"`
	Key         string  `json:"idempotency_key"`
	Confidence  *int64  `json:"confidence"`
	Seconds     *int64  `json:"seconds"`
	Answer      *string `json:"answer"`
	Note        *string `json:"note"`
	EventID     int64   `json:"study_event_id"`
	QuestionKey string  `json:"question_key"`
	Set         string  `json:"set_hash"`
	Revision    string  `json:"blueprint_revision_hash"`
}

// PracticeProgress is the per-question attempt aggregate. Attempts counts
// every recorded attempt; Streak counts consecutive correct answers from the
// latest attempt; Last and AttemptedAt describe the latest attempt (nil when
// the question has no attempts). AttemptedAt is stored timestamp text.
type PracticeProgress struct {
	Question    string  `json:"question_id"`
	Attempts    int64   `json:"attempts"`
	Streak      int64   `json:"correct_streak"`
	Last        *string `json:"last_outcome"`
	AttemptedAt *string `json:"last_attempted_at"`
}

// PracticeAdmission is the admitted current-question identity selected under
// the course/generation locks. Payload is the question payload when within
// the 64 KiB budget, nil otherwise; Evidence is the roadmap content bytes.
type PracticeAdmission struct {
	Unit     string  `json:"unit_key"`
	Key      string  `json:"question_key"`
	Set      string  `json:"set_hash"`
	Revision string  `json:"blueprint_revision_hash"`
	Payload  *string `json:"payload_json"`
	Evidence []byte  `json:"evidence"`
}

// PracticeAdmissionSelector scopes admission to the configured analysis
// generation/version/model and the requested question. Generation and Config
// pin the published navigation snapshot exactly.
type PracticeAdmissionSelector struct {
	CourseID        int64  `json:"course_id"`
	Generation      int64  `json:"source_generation"`
	Config          string `json:"config_generation"`
	AnalysisVersion string `json:"analysis_version"`
	Model           string `json:"model"`
	Question        string `json:"question_id"`
}

// PracticeAttemptParams records one attempt and its learner event atomically.
// Score is the configured outcome score; Confidence, Seconds, Answer and Note
// are nil when unset. Answer/Note are identity-encoded before the call.
type PracticeAttemptParams struct {
	CourseID   int64   `json:"course_id"`
	Unit       string  `json:"unit_key"`
	Question   string  `json:"question_id"`
	Key        string  `json:"question_key"`
	Set        string  `json:"set_hash"`
	Revision   string  `json:"blueprint_revision_hash"`
	Outcome    string  `json:"outcome"`
	Confidence *int64  `json:"confidence"`
	Seconds    *int64  `json:"seconds"`
	Score      float64 `json:"score"`
	Answer     *string `json:"answer"`
	Note       *string `json:"note"`
	IdemKey    string  `json:"idempotency_key"`
	EventKey   string  `json:"event_idempotency_key"`
}

// PracticeSetStatusCount aggregates chosen sets by status. Sets counts chosen
// sets; Questions sums their stored question rows.
type PracticeSetStatusCount struct {
	Status    string `json:"status"`
	Sets      int64  `json:"sets"`
	Questions int64  `json:"questions"`
}

// Practice is the typed port for validated practice questions, learner
// attempts and their aggregates. Selection matches the configured model and
// analysis version exactly; publication matches the pinned navigation
// snapshot exactly. LockCourse, LockedGeneration, AdmissionQuestion and
// InsertAttempt run on the caller's transaction so locks and writes
// commit or roll back with the surrounding attempt.
type Practice interface {
	// LockCourse locks the visible course row (visible, or hidden with an
	// enabled exam plan) and reports ErrNoRows when it is not visible.
	// Backend row locks apply where available.
	LockCourse(ctx context.Context, course int64) error
	// LockedGeneration returns the course generation under a shared lock.
	// It reports ErrNoRows when the course has no generation row.
	LockedGeneration(ctx context.Context, course int64) (int64, error)
	// ListQuestionSets returns the chosen set per unit: ready preferred,
	// then newest, capped at 201 rows in unit-key order.
	ListQuestionSets(ctx context.Context, selector PracticeSetSelector) ([]PracticeQuestionSet, error)
	// ListQuestions returns the stored questions for the given set ids in
	// (set id, ordinal) order, capped at 2401 rows.
	ListQuestions(ctx context.Context, setIDs []int64) ([]PracticeQuestionRow, error)
	// QuestionProgress aggregates attempts per requested question id.
	// Questions without attempts are omitted.
	QuestionProgress(ctx context.Context, course int64, questions []string) ([]PracticeProgress, error)
	// AdmissionQuestion selects the current question pinned to the usable
	// published snapshot. It reports ErrNoRows when the question is not
	// current, so callers can map it to a conflict.
	AdmissionQuestion(ctx context.Context, selector PracticeAdmissionSelector) (PracticeAdmission, error)
	// AttemptByKey returns the stored idempotent attempt row. It reports
	// ErrNoRows when no attempt used the key.
	AttemptByKey(ctx context.Context, key string) (PracticeAttemptRow, error)
	// InsertAttempt inserts the learner event and the attempt in one atomic
	// step and returns their ids as (attempt id, event id).
	InsertAttempt(ctx context.Context, params PracticeAttemptParams) (int64, int64, error)
	// SetStatusCounts aggregates the chosen sets (same selection as
	// ListQuestionSets, restricted to the given units) by status.
	SetStatusCounts(ctx context.Context, selector PracticeSetSelector, units []string) ([]PracticeSetStatusCount, error)
}
