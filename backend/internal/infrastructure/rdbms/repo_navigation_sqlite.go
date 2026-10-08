package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteNavigation struct{ db nativeDBTX }

// navigationActionsSQLite shares the action/progress join across the progress
// reads. Status derives from the latest learner event; the unit counts use
// count(CASE...) so both backends aggregate identically. SQLite takes no row
// locks: readers run on their snapshot and writers hold the admitted writer
// transaction.
const navigationActionsSQLite = `WITH actions AS (
 SELECT a.action_id,a.unit_key,a.ordinal,a.payload,
 CASE WHEN p.latest_event IN('completed','stuck','deferred')
 THEN p.latest_event WHEN p.latest_event IS NOT NULL
 THEN 'in_progress' ELSE 'pending' END status,
 coalesce(p.progress_minutes,0) progress_minutes
 FROM roadmap_actions a
 LEFT JOIN action_progress p
 ON p.course_id=a.course_id AND p.action_id=a.action_id
 WHERE a.course_id=?
)`

const navigationPayloadSQLite = `SELECT n.overview,content.payload,
coalesce(n.source_generation,-1),g.generation,
coalesce(n.config_generation,'')
FROM course_generation g
LEFT JOIN navigation n ON n.course_id=g.course_id
LEFT JOIN roadmap_content content
ON ? AND content.content_id=n.content_id WHERE g.course_id=?`

const navigationUnitsSQLite = navigationActionsSQLite + ` SELECT unit_key,
count(*) total_actions,
count(CASE WHEN status='completed' THEN 1 END) completed_actions
FROM actions GROUP BY unit_key`

const navigationNextSQLite = navigationActionsSQLite + ` SELECT payload,
status,progress_minutes FROM actions WHERE status<>'completed'
ORDER BY CASE WHEN status IN('pending','in_progress') THEN 0 ELSE 1 END,
ordinal LIMIT 1`

const navigationActionsAllSQLite = navigationActionsSQLite + ` SELECT payload,
status,progress_minutes FROM actions ORDER BY ordinal`

const navigationActionsUnitSQLite = navigationActionsSQLite + ` SELECT payload,
status,progress_minutes FROM actions WHERE unit_key=? ORDER BY ordinal`

const navigationStaleSQLite = `SELECT c.id,g.generation
FROM courses c JOIN course_generation g ON g.course_id=c.id
LEFT JOIN navigation n ON n.course_id=c.id
WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans p
WHERE p.course_id=c.id AND p.enabled=1))
AND (n.course_id IS NULL OR n.source_generation<>g.generation
OR n.config_generation<>?)
ORDER BY n.generated_at IS NOT NULL,n.generated_at,c.id LIMIT 1`

const navigationHistorySQLite = `SELECT id,revision,revision_hash,status,
model,attempts,created_at,generated_at FROM course_blueprints
WHERE course_id=? AND analysis_version=? AND requested_model=?
ORDER BY revision DESC LIMIT 500`

const navigationBlueprintSQLite = `SELECT
CASE WHEN length(CAST(payload_json AS BLOB))<=8388608
THEN payload_json END,
CASE WHEN length(CAST(evidence_packet_json AS BLOB))<=8388608
THEN evidence_packet_json END
FROM course_blueprints WHERE id=?`

const navigationEvidenceSQLite = `SELECT d.id,d.source_hash,d.document_kind,
d.display_name,d.source_path,d.source_origin,coalesce(d.source_url,''),
coalesce(e.status,''),coalesce(e.source_hash,''),
coalesce(e.analysis_version,''),coalesce(e.model,''),
coalesce(e.requested_model,e.model,''),coalesce(e.context_hash,''),
CASE WHEN length(CAST(e.payload_json AS BLOB))<=4194304
THEN coalesce(e.payload_json,'') ELSE '' END
FROM documents d LEFT JOIN document_enrichments e ON e.document_id=d.id
WHERE d.course_id=? AND d.id IN`

const navigationEvidenceSuffixSQLite = ` AND d.status='ready'
AND d.content_hash_verified=1 AND `

const navigationAdmitSQLite = `SELECT a.unit_key
FROM navigation n JOIN roadmap_actions a ON a.course_id=n.course_id
WHERE n.course_id=? AND a.action_id=? AND n.source_generation=?
AND n.config_generation=? AND n.revision_id=?
AND json_extract(n.overview,'$.usable')=1`

const navigationUpsertSQLite = `INSERT INTO navigation
(course_id,source_generation,config_generation,revision_id,overview,content_id)
VALUES(?,?,?,?,?,?) ON CONFLICT(course_id) DO UPDATE SET
source_generation=excluded.source_generation,
config_generation=excluded.config_generation,revision_id=excluded.revision_id,
overview=excluded.overview,content_id=excluded.content_id,
generated_at=strftime('%Y-%m-%d %H:%M:%S','now')`

const navigationPruneSQLite = `DELETE FROM roadmap_content
WHERE content_id=? AND NOT EXISTS(SELECT 1 FROM navigation n
WHERE n.content_id=roadmap_content.content_id)`

func (n sqliteNavigation) Payload(
	ctx context.Context, course int64, roadmap bool,
) (database.NavigationPayload, error) {
	var out database.NavigationPayload
	// SQLite has no boolean parameters: the roadmap flag binds as 0/1 and the
	// content join tests it directly.
	roadmapFlag := 0
	if roadmap {
		roadmapFlag = 1
	}
	err := n.db.QueryRow(ctx, navigationPayloadSQLite, roadmapFlag,
		course).Scan(&out.Overview, &out.Content, &out.Built,
		&out.Current, &out.Config)
	return out, err
}

func (n sqliteNavigation) UnitProgress(
	ctx context.Context, course int64,
) ([]database.NavigationUnitProgress, error) {
	rows, err := n.db.Query(ctx, navigationUnitsSQLite, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.NavigationUnitProgress{}
	for rows.Next() {
		var item database.NavigationUnitProgress
		if err := rows.Scan(&item.Key, &item.Total,
			&item.Complete); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (n sqliteNavigation) NextAction(
	ctx context.Context, course int64,
) (database.NavigationActionProgress, error) {
	var out database.NavigationActionProgress
	err := n.db.QueryRow(ctx, navigationNextSQLite, course).
		Scan(&out.Payload, &out.Status, &out.Minutes)
	return out, err
}

func (n sqliteNavigation) ListActions(
	ctx context.Context, course int64, unit *string,
) ([]database.NavigationActionProgress, error) {
	query := navigationActionsAllSQLite
	args := []any{course}
	if unit != nil {
		query = navigationActionsUnitSQLite
		args = append(args, *unit)
	}
	rows, err := n.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.NavigationActionProgress{}
	for rows.Next() {
		var item database.NavigationActionProgress
		if err := rows.Scan(&item.Payload, &item.Status,
			&item.Minutes); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (n sqliteNavigation) StudyDistribution(
	ctx context.Context, course int64,
) (map[int64]int64, error) {
	rows, err := n.db.Query(ctx, `SELECT max(0,min(5,level)),
count(*) FROM file_study WHERE course_id=? GROUP BY 1`, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var level, count int64
		if err := rows.Scan(&level, &count); err != nil {
			return nil, err
		}
		out[level] = count
	}
	return out, rows.Err()
}

func (n sqliteNavigation) StaleTarget(
	ctx context.Context, config string,
) (database.NavigationStaleTarget, error) {
	var out database.NavigationStaleTarget
	// Missing rows sort before any timestamp, like NULLS FIRST.
	err := n.db.QueryRow(ctx, navigationStaleSQLite, config).
		Scan(&out.CourseID, &out.Generation)
	return out, err
}

func (n sqliteNavigation) BlueprintHistory(
	ctx context.Context, course int64, version, model string,
) ([]database.NavigationHistoryRow, error) {
	rows, err := n.db.Query(ctx, navigationHistorySQLite, course,
		version, model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.NavigationHistoryRow{}
	for rows.Next() {
		var item database.NavigationHistoryRow
		if err := rows.Scan(&item.ID, &item.Number, &item.Hash,
			&item.Status, &item.Model, &item.Attempts,
			&item.Created, &item.Generated); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (n sqliteNavigation) BlueprintPayload(
	ctx context.Context, id int64,
) (database.NavigationBlueprintPayload, error) {
	var out database.NavigationBlueprintPayload
	// length(CAST(... AS BLOB)) counts payload bytes natively, matching the
	// PostgreSQL octet_length budget.
	err := n.db.QueryRow(ctx, navigationBlueprintSQLite, id).
		Scan(&out.Payload, &out.Packet)
	return out, err
}

func (n sqliteNavigation) EvidenceRows(
	ctx context.Context, course int64, ids []string,
) ([]database.NavigationEvidenceRow, error) {
	if len(ids) == 0 {
		return []database.NavigationEvidenceRow{}, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, course)
	for _, id := range ids {
		args = append(args, id)
	}
	query := navigationEvidenceSQLite + sqlitePlaceholders(len(ids)) +
		navigationEvidenceSuffixSQLite + documentAdmissionSQLite
	rows, err := n.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.NavigationEvidenceRow{}
	for rows.Next() {
		var item database.NavigationEvidenceRow
		if err := rows.Scan(&item.ID, &item.Hash, &item.Kind,
			&item.Name, &item.Path, &item.Origin, &item.URL,
			&item.AnalysisStatus, &item.AnalysisHash,
			&item.Version, &item.Model, &item.RequestedModel,
			&item.Context, &item.Payload); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (n sqliteNavigation) LockedGeneration(
	ctx context.Context, course int64,
) (int64, error) {
	// The admitted writer transaction (or the reader snapshot) provides the
	// exclusion PostgreSQL expresses with FOR SHARE.
	var generation int64
	err := n.db.QueryRow(ctx, `SELECT generation FROM course_generation
WHERE course_id=?`, course).Scan(&generation)
	return generation, err
}

func (n sqliteNavigation) AdmitActionUnit(
	ctx context.Context, admission database.NavigationAdmission,
) (string, error) {
	var unit string
	// JSON1 json_extract reads the TEXT overview payload natively; the
	// publisher writes usable as a JSON boolean.
	err := n.db.QueryRow(ctx, navigationAdmitSQLite, admission.CourseID,
		admission.Action, admission.Generation, admission.Config,
		admission.Revision).Scan(&unit)
	return unit, err
}

func (n sqliteNavigation) PublishGeneration(
	ctx context.Context, course int64,
) (int64, error) {
	// Publication runs on the admitted writer transaction, which serializes
	// stronger than FOR UPDATE.
	var generation int64
	err := n.db.QueryRow(ctx, `SELECT generation FROM course_generation
WHERE course_id=?`, course).Scan(&generation)
	return generation, err
}

func (n sqliteNavigation) PublishNavigation(
	ctx context.Context, params database.NavigationPublishParams,
) error {
	// Payloads bind as TEXT so JSON1 reads (json_extract) see JSON text.
	if _, err := n.db.Exec(ctx, `INSERT INTO roadmap_content
(content_id,payload) VALUES(?,?) ON CONFLICT DO NOTHING`,
		params.ContentID, string(params.Encoded)); err != nil {
		return err
	}
	var previous *string
	err := n.db.QueryRow(ctx, `SELECT content_id FROM navigation
WHERE course_id=?`, params.CourseID).Scan(&previous)
	if err != nil && !database.IsNoRows(err) {
		return err
	}
	if _, err := n.db.Exec(ctx, navigationUpsertSQLite, params.CourseID,
		params.Generation, params.Config, params.Revision,
		string(params.Overview), params.ContentID); err != nil {
		return err
	}
	if _, err := n.db.Exec(ctx, `DELETE FROM roadmap_actions
WHERE course_id=?`, params.CourseID); err != nil {
		return err
	}
	for ordinal, action := range params.Actions {
		if _, err := n.db.Exec(ctx, `INSERT INTO roadmap_actions
(course_id,action_id,ordinal,unit_key,payload) VALUES(?,?,?,?,?)`,
			params.CourseID, action.ID, ordinal, action.Unit,
			string(action.Payload)); err != nil {
			return err
		}
	}
	if previous == nil || *previous == params.ContentID {
		return nil
	}
	_, err = n.db.Exec(ctx, navigationPruneSQLite, *previous)
	return err
}
