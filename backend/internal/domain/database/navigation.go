package database

import "context"

// NavigationPayload is the generation-checked plan payload for one course.
// Overview is the stored overview bytes (nil when no navigation row is
// published); Content is the stored roadmap payload only when the caller
// requested the roadmap, nil otherwise. Built is the published source
// generation (-1 when nothing is published), Current the live source
// generation, Config the published config generation (empty when nothing is
// published).
type NavigationPayload struct {
	Overview []byte `json:"overview"`
	Content  []byte `json:"content"`
	Built    int64  `json:"source_generation"`
	Current  int64  `json:"generation"`
	Config   string `json:"config_generation"`
}

// NavigationStaleTarget is the oldest course whose published navigation no
// longer matches its source or config generation.
type NavigationStaleTarget struct {
	CourseID   int64 `json:"course_id"`
	Generation int64 `json:"generation"`
}

// NavigationUnitProgress aggregates live learner progress for one unit key.
// Key stays encoded exactly as stored.
type NavigationUnitProgress struct {
	Key      string `json:"unit_key"`
	Total    int64  `json:"total_actions"`
	Complete int64  `json:"completed_actions"`
}

// NavigationActionProgress is one ordered action payload joined to its live
// learner status. Payload stays encoded exactly as stored.
type NavigationActionProgress struct {
	Payload []byte `json:"payload"`
	Status  string `json:"status"`
	Minutes int64  `json:"progress_minutes"`
}

// NavigationHistoryRow is one stored blueprint revision row for the configured
// analysis version and requested model. Created and Generated are stored
// timestamp text; Generated is nil when unset.
type NavigationHistoryRow struct {
	ID        int64   `json:"id"`
	Number    int64   `json:"revision"`
	Hash      string  `json:"revision_hash"`
	Status    string  `json:"status"`
	Model     string  `json:"model"`
	Attempts  int64   `json:"attempts"`
	Created   string  `json:"created_at"`
	Generated *string `json:"generated_at"`
}

// NavigationBlueprintPayload is the stored blueprint and evidence packet for
// one revision, each nil when over the 8 MiB budget or absent.
type NavigationBlueprintPayload struct {
	Payload *string `json:"payload_json"`
	Packet  *string `json:"evidence_packet_json"`
}

// NavigationEvidenceRow is one admitted ready source document joined to its
// current enrichment. Stored text stays encoded exactly as stored; Payload is
// the enrichment JSON only within the 4 MiB budget, empty otherwise.
type NavigationEvidenceRow struct {
	ID             string `json:"document_id"`
	Hash           string `json:"source_hash"`
	Kind           string `json:"document_kind"`
	Name           string `json:"display_name"`
	Path           string `json:"source_path"`
	Origin         string `json:"source_origin"`
	URL            string `json:"source_url"`
	AnalysisStatus string `json:"enrichment_status"`
	AnalysisHash   string `json:"enrichment_source_hash"`
	Version        string `json:"enrichment_analysis_version"`
	Model          string `json:"enrichment_model"`
	RequestedModel string `json:"enrichment_requested_model"`
	Context        string `json:"enrichment_context_hash"`
	Payload        string `json:"enrichment_payload"`
}

// NavigationAdmission selects one action pinned to the usable published
// snapshot. Generation and Config pin the course source generation and the
// configured analysis generation exactly; Action and Revision are
// identity-encoded before the call.
type NavigationAdmission struct {
	CourseID   int64  `json:"course_id"`
	Generation int64  `json:"source_generation"`
	Config     string `json:"config_generation"`
	Action     string `json:"action_id"`
	Revision   string `json:"revision_id"`
}

// NavigationAction is one immutable action row to publish. ID and Unit are
// stored verbatim; Payload is the caller-encoded action bytes.
type NavigationAction struct {
	ID      string `json:"action_id"`
	Unit    string `json:"unit_key"`
	Payload []byte `json:"payload"`
}

// NavigationPublishParams replaces the immutable navigation content and action
// membership for one course. Revision is the stored revision id (nil when the
// content carries none); ContentID is the caller-computed content hash;
// Encoded is the full content bytes and Overview the compact overview bytes,
// both identity-encoded by the caller. Actions carries the replacement
// membership in publish order; payloads stay encoded exactly as supplied.
type NavigationPublishParams struct {
	CourseID   int64              `json:"course_id"`
	Generation int64              `json:"source_generation"`
	Config     string             `json:"config_generation"`
	Revision   *string            `json:"revision_id"`
	ContentID  string             `json:"content_id"`
	Encoded    []byte             `json:"encoded"`
	Overview   []byte             `json:"overview"`
	Actions    []NavigationAction `json:"actions"`
}

// Navigation is the typed port for the published course-plan projection:
// generation-checked reads, live learner progress, blueprint history, source
// evidence admission, action admission and atomic publication. Generation and
// config comparisons stay in the domain; adapters only return stamps and
// pinned rows. LockedGeneration, AdmitActionUnit, PublishGeneration and
// PublishNavigation run on the caller's transaction so locks and writes
// commit or roll back with the surrounding work.
type Navigation interface {
	// Payload returns the generation-checked plan payload for one course. It
	// reports ErrNoRows when the course has no generation row. Content is
	// populated only when roadmap is true.
	Payload(ctx context.Context, course int64, roadmap bool) (NavigationPayload, error)
	// UnitProgress aggregates live action progress per unit key for one
	// course, in no guaranteed order.
	UnitProgress(ctx context.Context, course int64) ([]NavigationUnitProgress, error)
	// NextAction returns the next incomplete action payload: pending and
	// in-progress first, then other incomplete states, each by ordinal. It
	// reports ErrNoRows when every action is completed or none exists.
	NextAction(ctx context.Context, course int64) (NavigationActionProgress, error)
	// ListActions returns ordered action payloads for one course, optionally
	// restricted to one encoded unit key. A nil unit selects every unit.
	ListActions(ctx context.Context, course int64, unit *string) ([]NavigationActionProgress, error)
	// StudyDistribution aggregates file-study levels clamped to [0,5] as a
	// level-to-count map for one course.
	StudyDistribution(ctx context.Context, course int64) (map[int64]int64, error)
	// StaleTarget returns the oldest visible (or exam-planned) course whose
	// navigation is missing or whose source/config generation differs. It
	// reports ErrNoRows when every projection is current.
	StaleTarget(ctx context.Context, config string) (NavigationStaleTarget, error)
	// BlueprintHistory returns stored blueprint revisions for the configured
	// analysis version and requested model, newest first, capped at 500.
	BlueprintHistory(ctx context.Context, course int64, version, model string) ([]NavigationHistoryRow, error)
	// BlueprintPayload returns the stored blueprint and evidence packet for
	// one revision id. It reports ErrNoRows when the revision is missing.
	BlueprintPayload(ctx context.Context, id int64) (NavigationBlueprintPayload, error)
	// EvidenceRows returns admitted ready source rows for the requested
	// document ids of one course. Only rows passing the source boundary
	// (current membership, live catalog revision, admitted archive
	// ancestry) are returned; unknown or unadmitted ids are omitted.
	EvidenceRows(ctx context.Context, course int64, ids []string) ([]NavigationEvidenceRow, error)
	// LockedGeneration returns the course generation under a shared lock. It
	// reports ErrNoRows when the course has no generation row.
	LockedGeneration(ctx context.Context, course int64) (int64, error)
	// AdmitActionUnit returns the stored unit key for an action pinned to
	// the usable published snapshot under shared locks. It reports
	// ErrNoRows when the action is not current, so callers can map it to a
	// conflict. The returned key stays encoded exactly as stored.
	AdmitActionUnit(ctx context.Context, admission NavigationAdmission) (string, error)
	// PublishGeneration locks the course generation row for publication and
	// returns the live generation. It reports ErrNoRows when the course
	// has no generation row.
	PublishGeneration(ctx context.Context, course int64) (int64, error)
	// PublishNavigation atomically replaces the navigation row and action
	// membership for one pinned generation and prunes the superseded
	// content when nothing references it.
	PublishNavigation(ctx context.Context, params NavigationPublishParams) error
}
