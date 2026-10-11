package rdbms

import (
	"time"

	"tree-eclass/internal/domain/database"
)

// analysisEligibleSQLite is the servable-document boundary for alias d joined to
// course alias c: admitted source, ready verified non-archive content, and a
// visible course (or a hidden course with an enabled exam plan).
const analysisEligibleSQLite = documentAdmissionSQLite +
	` AND d.status='ready' AND d.content_hash_verified=1 AND d.document_kind<>'archive'` +
	` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans plan` +
	` WHERE plan.course_id=c.id AND plan.enabled=1))`

// analysisDueSQLite matches rows whose stored TEXT availability has passed.
// datetime() truncates both sides to whole seconds: a float julianday
// comparison misorders freshly written RFC3339Nano stamps against the
// millisecond-precision now() about half the time, while second precision
// is monotonic for due checks and claims at most a second early.
func analysisDueSQLite(column string) string {
	return "datetime(" + column + ")<=datetime('now')"
}

type sqliteAnalysis struct{ db nativeDBTX }

// analysisCandidateRow is one ordered enrichment candidate for Go-side due
// evaluation, where hashing and JSON text rules have no native equivalent.
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
// database port helper.
func analysisContextHash(id string, course int64, hash, kind, name, courseName, path, origin string) string {
	return database.AnalysisContextHash(id, course, hash, kind, name, courseName, path, origin)
}

func scanExcerpts(rows nativeRows) ([]database.ExcerptRow, error) {
	out := []database.ExcerptRow{}
	for rows.Next() {
		var item database.ExcerptRow
		if err := rows.Scan(&item.LocatorType, &item.LocatorStart, &item.Text); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
