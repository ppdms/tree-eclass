package rdbms

import (
	"strconv"
	"time"

	"tree-eclass/internal/domain/database"
)

// analysisEligiblePG is the servable-document boundary for alias d joined to
// course alias c: admitted source, ready verified non-archive content, and a
// visible course (or a hidden course with an enabled exam plan).
const analysisEligiblePG = documentAdmissionPG +
	` AND d.status='ready' AND d.content_hash_verified=1 AND d.document_kind<>'archive'` +
	` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans plan` +
	` WHERE plan.course_id=c.id AND plan.enabled=1))`

// analysisEligibleSQLite is the same boundary with unqualified table names.
const analysisEligibleSQLite = documentAdmissionSQLite +
	` AND d.status='ready' AND d.content_hash_verified=1 AND d.document_kind<>'archive'` +
	` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans plan` +
	` WHERE plan.course_id=c.id AND plan.enabled=1))`

// analysisDuePG matches rows whose stored TEXT availability has passed at the
// backend clock. Stored stamps are RFC3339Nano UTC text or legacy
// space-separated UTC text; both cast to timestamptz for the comparison.
func analysisDuePG(column string) string {
	return column + "::timestamptz<=clock_timestamp()"
}

// analysisDueSQLite is the same due check over normalized julianday values.
func analysisDueSQLite(column string) string {
	return "julianday(" + column + ")<=julianday('now')"
}

// analysisVisualDuePG reports whether a visual document still needs page
// work: non-visual kinds are always due, otherwise any missing, stale, or
// due pending page row makes it due, as does a complete ready page set. The
// model binds at $model, the page version at $page.
func analysisVisualDuePG(model, page int) string {
	m, p := strconv.Itoa(model), strconv.Itoa(page)
	return `(d.document_kind NOT IN('pdf','image')` +
		` OR NOT EXISTS(SELECT 1 FROM knowledge.page_enrichments p WHERE p.document_id=d.id)` +
		` OR EXISTS(SELECT 1 FROM knowledge.page_enrichments p WHERE p.document_id=d.id` +
		` AND (p.source_hash<>d.source_hash OR p.requested_model<>$` + m +
		` OR p.analysis_version<>$` + p +
		` OR (p.status='pending' AND ` + analysisDuePG("p.available_at") + `)))` +
		` OR (SELECT count(*) FROM knowledge.page_enrichments p WHERE p.document_id=d.id` +
		` AND p.status='ready' AND p.source_hash=d.source_hash AND p.requested_model=$` + m +
		` AND p.analysis_version=$` + p +
		` AND p.page_number BETWEEN 1 AND d.page_count)=d.page_count)`
}

type postgresAnalysis struct{ db nativeDBTX }

type sqliteAnalysis struct{ db nativeDBTX }

// analysisContextPG renders the canonical document identity hash expression
// for row aliases: sha256 hex over the jsonb array text of
// (id, course, hash, kind, display, course-name, path, origin).
// PostgreSQL 18 ships sha256(bytea) in core; no extension is required.
const analysisContextPG = `encode(sha256(convert_to(jsonb_build_array(d.id,d.course_id,d.source_hash,` +
	`d.document_kind,d.display_name,c.name,d.normalized_path,d.source_origin)::text,'UTF8')),'hex')`

// analysisCandidateRow is one ordered enrichment candidate for Go-side due
// evaluation on SQLite, where hashing and JSON text rules have no native
// equivalent.
type analysisCandidateRow struct {
	id, hash, kind, name, courseName, path, origin string
	course                                         int64
	pages                                          *int64
	indexed                                        *string
	enrichment                                     *string
	source, context                                *string
	requested, model, version, status, available   *string
	priority                                       *int64
	pending, pendingDue                            bool
}

func analysisCandidatePointers(row *analysisCandidateRow) []any {
	return []any{
		&row.id, &row.course, &row.hash, &row.kind, &row.name, &row.courseName,
		&row.path, &row.origin, &row.pages, &row.indexed, &row.enrichment,
		&row.source, &row.context, &row.requested, &row.model, &row.version,
		&row.status, &row.available, &row.priority,
	}
}

// staleInputs reports the hash/model/version mismatch arm of the candidate
// predicate, recording the pending/due state for the visual-due arm.
func (row *analysisCandidateRow) staleInputs(model string, versions database.AnalysisVersions) bool {
	row.pending = row.enrichment != nil && row.status != nil && *row.status == "pending"
	row.pendingDue = row.pending && analysisDueText(row.available)
	if row.enrichment == nil {
		return true
	}
	if row.source == nil || *row.source != row.hash {
		return true
	}
	if row.context == nil || *row.context != analysisContextHash(row.id, row.course,
		row.hash, row.kind, row.name, row.courseName, row.path, row.origin) {
		return true
	}
	requested := row.model
	if row.requested != nil {
		requested = row.requested
	}
	if requested == nil || *requested != model {
		return true
	}
	want := versions.Document
	if row.kind == "pdf" || row.kind == "image" {
		want = versions.Synthesis
	}
	return row.version == nil || *row.version != want
}

// analysisDueText matches the SQLite due check for stored TEXT stamps:
// RFC3339Nano or legacy space-separated UTC text at or before now.
func analysisDueText(stamp *string) bool {
	if stamp == nil {
		return false
	}
	text := *stamp
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if instant, err := time.Parse(layout, text); err == nil {
			return !instant.After(time.Now().UTC())
		}
	}
	return false
}

// analysisContextHash renders the canonical identity hash via the shared
// database port helper so SQLite evaluation matches PostgreSQL byte for byte.
func analysisContextHash(id string, course int64, hash, kind, name, courseName, path, origin string) string {
	return database.AnalysisContextHash(id, course, hash, kind, name, courseName, path, origin)
}
