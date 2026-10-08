// Package database defines SQL-free persistence contracts and neutral values.
package database

import (
	"context"
	"errors"
)

// ErrPacketTooLarge reports a stored evidence packet over the 2 MiB budget.
var ErrPacketTooLarge = errors.New("database: synthesis packet exceeds 2 MiB")

// SynthesisLane selects the synthesis output family. The service never names
// tables; adapters map each lane to its native blueprint or practice tables.
type SynthesisLane int

const (
	// SynthesisCourse builds course blueprints.
	SynthesisCourse SynthesisLane = iota + 1
	// SynthesisPractice builds practice question sets.
	SynthesisPractice
)

// String renders the lane name stored in synthesis_scan and logs.
func (lane SynthesisLane) String() string {
	if lane == SynthesisPractice {
		return "practice"
	}
	return "course"
}

// SynthesisLaneFor parses a lane name, reporting false for unknown lanes.
func SynthesisLaneFor(name string) (SynthesisLane, bool) {
	switch name {
	case "course":
		return SynthesisCourse, true
	case "practice":
		return SynthesisPractice, true
	default:
		return 0, false
	}
}

// SynthesisClaim is one due pending row selected for generation. Attempts is
// the stored attempt count before this claim; ClaimedAt is the claim stamp the
// caller committed.
type SynthesisClaim struct {
	ID       int64 `json:"id"`
	CourseID int64 `json:"course_id"`
	Attempts int64 `json:"attempts"`
}

// SynthesisPacket is the locked pending payload for one claim. Hash is the
// lane revision hash (course revision_hash, practice set_hash); UnitKey and
// BlueprintHash are empty for the course lane. PacketJSON is the stored
// evidence packet (never over the 2 MiB budget). Attempts is the stored count
// before this claim.
type SynthesisPacket struct {
	Hash       string `json:"hash"`
	UnitKey    string `json:"unit_key"`
	Blueprint  string `json:"blueprint_revision_hash"`
	Attempts   int64  `json:"attempts"`
	PacketJSON string `json:"evidence_packet_json"`
}

// SynthesisEvidence counts admitted documents and ready insights for a course.
type SynthesisEvidence struct {
	Total int64 `json:"documents_total"`
	Ready int64 `json:"documents_with_ready_insight"`
}

// SynthesisDocument is one ranked ready-insight document row. Payload is the
// stored insight JSON (never over 1 MiB); stored text stays encoded.
type SynthesisDocument struct {
	ID           string `json:"document_id"`
	SourceHash   string `json:"source_hash"`
	DisplayName  string `json:"display_name"`
	SourcePath   string `json:"source_path"`
	SourceOrigin string `json:"source_origin"`
	DocumentKind string `json:"document_kind"`
	AnalysisHash string `json:"enrichment_source_hash"`
	Version      string `json:"enrichment_analysis_version"`
	Model        string `json:"enrichment_model"`
	Requested    string `json:"enrichment_requested_model"`
	ContextHash  string `json:"enrichment_context_hash"`
	Payload      string `json:"payload_json"`
}

// SynthesisExcerpt is one chunk excerpt row. Start is nil for NULL locators;
// Text is truncated to 1200 runes natively. Stored text stays encoded.
type SynthesisExcerpt struct {
	Ordinal int64   `json:"ordinal"`
	Type    string  `json:"locator_type"`
	Start   *string `json:"locator_start"`
	Text    string  `json:"text"`
}

// SynthesisCommunityEntry is one mapped conversation header. GuildID is nil
// when the channel row is absent; stored names stay encoded.
type SynthesisCommunityEntry struct {
	ChannelName string `json:"channel_name"`
	EndedAt     string `json:"ended_at"`
	ChannelID   string `json:"channel_id"`
	GuildID     *int64 `json:"guild_id"`
}

// SynthesisCommunityMessage is one capped community message row. Stored author
// names and content stay encoded.
type SynthesisCommunityMessage struct {
	MessageID string `json:"message_id"`
	Timestamp string `json:"timestamp"`
	Author    string `json:"author_name"`
	Content   string `json:"content"`
}

// SynthesisBlueprint is the ready course blueprint seeding practice queues.
// Payload and Packet are nil when over their size caps (1 MiB / 2 MiB).
type SynthesisBlueprint struct {
	Revision string  `json:"revision_hash"`
	Payload  *string `json:"payload_json"`
	Packet   *string `json:"evidence_packet_json"`
}

// SynthesisQuestion carries one validated practice question at its ordinal.
type SynthesisQuestion struct {
	ID         string `json:"question_id"`
	Key        string `json:"question_key"`
	Mode       string `json:"response_mode"`
	Difficulty string `json:"difficulty"`
	Minutes    int64  `json:"estimated_minutes"`
	Payload    string `json:"payload_json"`
}

// SynthesisScan selects one scan-due course for a lane.
type SynthesisScan struct {
	CourseID int64 `json:"course_id"`
}

// QueueCourseParams files a course revision: stale out pending/running rows
// with a different hash, reactivate a stale identical hash, else insert.
type QueueCourseParams struct {
	CourseID     int64  `json:"course_id"`
	RevisionHash string `json:"revision_hash"`
	EvidenceHash string `json:"evidence_hash"`
	PacketJSON   string `json:"evidence_packet_json"`
	Version      string `json:"analysis_version"`
	Model        string `json:"requested_model"`
	AvailableAt  string `json:"available_at"`
}

// QueuePracticeParams files one practice revision for a unit.
type QueuePracticeParams struct {
	CourseID     int64  `json:"course_id"`
	UnitKey      string `json:"unit_key"`
	Revision     string `json:"blueprint_revision_hash"`
	SetHash      string `json:"set_hash"`
	EvidenceHash string `json:"evidence_hash"`
	PacketJSON   string `json:"evidence_packet_json"`
	Version      string `json:"analysis_version"`
	Model        string `json:"requested_model"`
	AvailableAt  string `json:"available_at"`
}

// PublishParams publishes one claimed result as the ready revision.
type PublishParams struct {
	ID        int64  `json:"id"`
	CourseID  int64  `json:"course_id"`
	UnitKey   string `json:"unit_key"`
	ClaimedAt string `json:"claimed_at"`
	Model     string `json:"model"`
	Payload   string `json:"payload_json"`
	ReadyAt   string `json:"generated_at"`
}

// FinishParams parks or fails one claimed row.
type FinishParams struct {
	ID          int64  `json:"id"`
	ClaimedAt   string `json:"claimed_at"`
	Status      string `json:"status"`
	Error       string `json:"error"`
	AvailableAt string `json:"available_at"`
	Reset       bool   `json:"reset_attempt"`
	FinishedAt  string `json:"finished_at"`
}

// SynthesisEvidenceParams scopes document evidence reads: requested model,
// document/page analysis versions and the ranked row cap.
type SynthesisEvidenceParams struct {
	CourseID    int64  `json:"course_id"`
	Model       string `json:"requested_model"`
	DocVersion  string `json:"document_analysis_version"`
	PageVersion string `json:"page_synthesis_version"`
	Limit       int64  `json:"limit"`
}

// SynthesisCommunityParams scopes one community term search.
type SynthesisCommunityParams struct {
	CourseID int64    `json:"course_id"`
	Terms    []string `json:"terms"`
	Limit    int64    `json:"limit"`
}

// Synthesis is the typed port for course/practice generation: scan cursors,
// durable claims, source-bound evidence reads, revision filing, publication
// and failure handling. Every method binds to the caller's transaction.
type Synthesis interface {
	// AcquireExpensive serializes expensive generation admission with the
	// Analysis and Indexing lanes. It requires a transaction-bound handle.
	AcquireExpensive(ctx context.Context) error
	// RecoverLane returns running rows to pending, spending one attempt.
	RecoverLane(ctx context.Context, lane SynthesisLane, availableAt string) error
	// ScanDueCourse selects one scan-due course, locking it against
	// concurrent scanners. It reports ErrNoRows when nothing is due.
	ScanDueCourse(ctx context.Context, lane SynthesisLane, config string) (SynthesisScan, error)
	// RecordScan advances the scan cursor for a course and lane.
	RecordScan(ctx context.Context, lane SynthesisLane, course int64, config, nextAt string) error
	// ClaimDueRow selects the due pending row for a model and version,
	// locking the course first. It reports ErrNoRows when nothing is due.
	ClaimDueRow(ctx context.Context, lane SynthesisLane, model, version string) (SynthesisClaim, error)
	// LoadPacket locks and returns the pending payload for a claimed row.
	// It reports ErrNoRows when the row is no longer pending and due.
	LoadPacket(ctx context.Context, lane SynthesisLane, id int64) (SynthesisPacket, error)
	// MarkClaimed moves a pending row to running and stamps the claim.
	MarkClaimed(ctx context.Context, lane SynthesisLane, id int64, claimedAt string) error
	// AbandonClaim parks a stale claim so scanning can reconsider it.
	AbandonClaim(ctx context.Context, lane SynthesisLane, id int64, finishedAt string) error
	// EvidenceCounts counts admitted documents and ready insights.
	EvidenceCounts(ctx context.Context, params SynthesisEvidenceParams) (SynthesisEvidence, error)
	// EvidenceDocuments lists ranked ready-insight documents, bounded.
	EvidenceDocuments(ctx context.Context, params SynthesisEvidenceParams) ([]SynthesisDocument, error)
	// DocumentExcerpts lists chunk excerpts ordered by ordinal.
	DocumentExcerpts(ctx context.Context, document string) ([]SynthesisExcerpt, error)
	// SearchCommunityIDs returns recent ready conversation IDs matching
	// every term, newest first, bounded.
	SearchCommunityIDs(ctx context.Context, params SynthesisCommunityParams) ([]string, error)
	// CommunityEntry loads one mapped conversation header.
	CommunityEntry(ctx context.Context, course int64, id string) (SynthesisCommunityEntry, error)
	// CommunityMessages loads capped community message rows for one entry.
	CommunityMessages(ctx context.Context, course int64, id string) ([]SynthesisCommunityMessage, error)
	// ReadyBlueprint returns the ready course blueprint seeding practice.
	// It reports ErrNoRows when no ready blueprint exists.
	ReadyBlueprint(ctx context.Context, course int64, model, version string) (SynthesisBlueprint, error)
	// BlueprintReady reports whether a course blueprint revision is ready.
	BlueprintReady(ctx context.Context, course int64, revision, model, version string) (bool, error)
	// QueueCourseRevision files a course revision (stale-out, reactivate,
	// insert) in one atomic step.
	QueueCourseRevision(ctx context.Context, params QueueCourseParams) error
	// QueuePracticeRevision files one practice revision in one atomic step.
	QueuePracticeRevision(ctx context.Context, params QueuePracticeParams) error
	// LockCourseForPublish holds the course row through publication. It
	// reports ErrNoRows when the course no longer exists.
	LockCourseForPublish(ctx context.Context, course int64) error
	// LockGenerationForPublish holds the course generation counter through
	// publication so concurrent source commits serialize.
	LockGenerationForPublish(ctx context.Context, course int64) error
	// ClaimActive reports whether a row is still claimed by this worker.
	ClaimActive(ctx context.Context, lane SynthesisLane, id int64, claimedAt string) (bool, error)
	// PublishReady retires the prior ready revision and publishes the
	// claimed row as ready.
	PublishReady(ctx context.Context, lane SynthesisLane, params PublishParams) error
	// ReplaceQuestions swaps the stored questions for a practice set.
	ReplaceQuestions(ctx context.Context, course int64, setID int64, unit string, questions []SynthesisQuestion) error
	// FinishClaim parks or fails one claimed row.
	FinishClaim(ctx context.Context, lane SynthesisLane, params FinishParams) error
}
