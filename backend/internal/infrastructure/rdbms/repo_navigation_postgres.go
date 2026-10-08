package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresNavigation struct{ db nativeDBTX }

// navigationActionsPG shares the action/progress join across the progress
// reads. Status derives from the latest learner event; the unit counts use
// count(CASE...) so both backends aggregate identically.
const navigationActionsPG = `WITH actions AS (
 SELECT a.action_id,a.unit_key,a.ordinal,a.payload,
 CASE WHEN p.latest_event IN('completed','stuck','deferred')
 THEN p.latest_event WHEN p.latest_event IS NOT NULL
 THEN 'in_progress' ELSE 'pending' END status,
 coalesce(p.progress_minutes,0) progress_minutes
 FROM read_model.roadmap_actions a
 LEFT JOIN read_model.action_progress p USING(course_id,action_id)
 WHERE a.course_id=$1
)`

const navigationPayloadPG = `SELECT n.overview,content.payload,
coalesce(n.source_generation,-1),g.generation,coalesce(n.config_generation,'')
FROM read_model.course_generation g
LEFT JOIN read_model.navigation n USING(course_id)
LEFT JOIN read_model.roadmap_content content
ON $2 AND content.content_id=n.content_id WHERE g.course_id=$1`

const navigationUnitsPG = navigationActionsPG + ` SELECT unit_key,
count(*) total_actions,
count(CASE WHEN status='completed' THEN 1 END) completed_actions
FROM actions GROUP BY unit_key`

const navigationNextPG = navigationActionsPG + ` SELECT payload,status,
progress_minutes FROM actions WHERE status<>'completed'
ORDER BY CASE WHEN status IN('pending','in_progress') THEN 0 ELSE 1 END,
ordinal LIMIT 1`

const navigationActionsAllPG = navigationActionsPG + ` SELECT payload,
status,progress_minutes FROM actions ORDER BY ordinal`

const navigationActionsUnitPG = navigationActionsPG + ` SELECT payload,
status,progress_minutes FROM actions WHERE unit_key=$2 ORDER BY ordinal`

const navigationStalePG = `SELECT c.id,g.generation
FROM app.courses c JOIN read_model.course_generation g ON g.course_id=c.id
LEFT JOIN read_model.navigation n ON n.course_id=c.id
WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p
WHERE p.course_id=c.id AND p.enabled=1))
AND (n.course_id IS NULL OR n.source_generation<>g.generation
OR n.config_generation<>$1)
ORDER BY n.generated_at NULLS FIRST,c.id LIMIT 1`

const navigationHistoryPG = `SELECT id,revision,revision_hash,status,model,
attempts,created_at,generated_at FROM knowledge.course_blueprints
WHERE course_id=$1 AND analysis_version=$2 AND requested_model=$3
ORDER BY revision DESC LIMIT 500`

const navigationBlueprintPG = `SELECT
CASE WHEN octet_length(payload_json)<=8388608 THEN payload_json END,
CASE WHEN octet_length(evidence_packet_json)<=8388608
THEN evidence_packet_json END
FROM knowledge.course_blueprints WHERE id=$1`

const navigationEvidencePG = `SELECT d.id,d.source_hash,d.document_kind,
d.display_name,d.source_path,d.source_origin,coalesce(d.source_url,''),
coalesce(e.status,''),coalesce(e.source_hash,''),
coalesce(e.analysis_version,''),coalesce(e.model,''),
coalesce(e.requested_model,e.model,''),coalesce(e.context_hash,''),
CASE WHEN octet_length(e.payload_json)<=4194304
THEN coalesce(e.payload_json,'') ELSE '' END
FROM knowledge.documents d
LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id
WHERE d.course_id=$1 AND d.id IN`

const navigationEvidenceSuffixPG = ` AND d.status='ready'
AND d.content_hash_verified=1 AND `

const navigationAdmitPG = `SELECT a.unit_key
FROM read_model.navigation n
JOIN read_model.roadmap_actions a USING(course_id)
WHERE n.course_id=$1 AND a.action_id=$2 AND n.source_generation=$3
AND n.config_generation=$4 AND n.revision_id=$5
AND n.overview->>'usable'='true' FOR SHARE OF n,a`

const navigationUpsertPG = `INSERT INTO read_model.navigation
(course_id,source_generation,config_generation,revision_id,overview,content_id)
VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(course_id) DO UPDATE SET
source_generation=excluded.source_generation,
config_generation=excluded.config_generation,revision_id=excluded.revision_id,
overview=excluded.overview,content_id=excluded.content_id,
generated_at=now()`

const navigationPrunePG = `DELETE FROM read_model.roadmap_content c
WHERE content_id=$1 AND NOT EXISTS(SELECT 1 FROM read_model.navigation n
WHERE n.content_id=c.content_id)`

func (n postgresNavigation) Payload(
	ctx context.Context, course int64, roadmap bool,
) (database.NavigationPayload, error) {
	var out database.NavigationPayload
	err := n.db.QueryRow(ctx, navigationPayloadPG, course, roadmap).
		Scan(&out.Overview, &out.Content, &out.Built,
			&out.Current, &out.Config)
	return out, err
}

func (n postgresNavigation) UnitProgress(
	ctx context.Context, course int64,
) ([]database.NavigationUnitProgress, error) {
	rows, err := n.db.Query(ctx, navigationUnitsPG, course)
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

func (n postgresNavigation) NextAction(
	ctx context.Context, course int64,
) (database.NavigationActionProgress, error) {
	var out database.NavigationActionProgress
	err := n.db.QueryRow(ctx, navigationNextPG, course).
		Scan(&out.Payload, &out.Status, &out.Minutes)
	return out, err
}

func (n postgresNavigation) ListActions(
	ctx context.Context, course int64, unit *string,
) ([]database.NavigationActionProgress, error) {
	query := navigationActionsAllPG
	args := []any{course}
	if unit != nil {
		query = navigationActionsUnitPG
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

func (n postgresNavigation) StudyDistribution(
	ctx context.Context, course int64,
) (map[int64]int64, error) {
	rows, err := n.db.Query(ctx, `SELECT greatest(0,least(5,level)),
count(*) FROM app.file_study WHERE course_id=$1 GROUP BY 1`, course)
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

func (n postgresNavigation) StaleTarget(
	ctx context.Context, config string,
) (database.NavigationStaleTarget, error) {
	var out database.NavigationStaleTarget
	err := n.db.QueryRow(ctx, navigationStalePG, config).
		Scan(&out.CourseID, &out.Generation)
	return out, err
}

func (n postgresNavigation) BlueprintHistory(
	ctx context.Context, course int64, version, model string,
) ([]database.NavigationHistoryRow, error) {
	rows, err := n.db.Query(ctx, navigationHistoryPG, course,
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

func (n postgresNavigation) BlueprintPayload(
	ctx context.Context, id int64,
) (database.NavigationBlueprintPayload, error) {
	var out database.NavigationBlueprintPayload
	err := n.db.QueryRow(ctx, navigationBlueprintPG, id).
		Scan(&out.Payload, &out.Packet)
	return out, err
}

func (n postgresNavigation) EvidenceRows(
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
	query := navigationEvidencePG + pgPlaceholders(2, len(ids)) +
		navigationEvidenceSuffixPG + documentAdmissionPG
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

func (n postgresNavigation) LockedGeneration(
	ctx context.Context, course int64,
) (int64, error) {
	var generation int64
	err := n.db.QueryRow(ctx, `SELECT generation
FROM read_model.course_generation WHERE course_id=$1 FOR SHARE`,
		course).Scan(&generation)
	return generation, err
}

func (n postgresNavigation) AdmitActionUnit(
	ctx context.Context, admission database.NavigationAdmission,
) (string, error) {
	var unit string
	err := n.db.QueryRow(ctx, navigationAdmitPG, admission.CourseID,
		admission.Action, admission.Generation, admission.Config,
		admission.Revision).Scan(&unit)
	return unit, err
}

func (n postgresNavigation) PublishGeneration(
	ctx context.Context, course int64,
) (int64, error) {
	var generation int64
	err := n.db.QueryRow(ctx, `SELECT generation
FROM read_model.course_generation WHERE course_id=$1 FOR UPDATE`,
		course).Scan(&generation)
	return generation, err
}

func (n postgresNavigation) PublishNavigation(
	ctx context.Context, params database.NavigationPublishParams,
) error {
	if _, err := n.db.Exec(ctx, `INSERT INTO read_model.roadmap_content
(content_id,payload) VALUES($1,$2) ON CONFLICT DO NOTHING`,
		params.ContentID, params.Encoded); err != nil {
		return err
	}
	var previous *string
	err := n.db.QueryRow(ctx, `SELECT content_id
FROM read_model.navigation WHERE course_id=$1`,
		params.CourseID).Scan(&previous)
	if err != nil && !database.IsNoRows(err) {
		return err
	}
	if _, err := n.db.Exec(ctx, navigationUpsertPG, params.CourseID,
		params.Generation, params.Config, params.Revision,
		params.Overview, params.ContentID); err != nil {
		return err
	}
	if _, err := n.db.Exec(ctx, `DELETE FROM read_model.roadmap_actions
WHERE course_id=$1`, params.CourseID); err != nil {
		return err
	}
	for ordinal, action := range params.Actions {
		if _, err := n.db.Exec(ctx, `INSERT INTO read_model.roadmap_actions
(course_id,action_id,ordinal,unit_key,payload) VALUES($1,$2,$3,$4,$5)`,
			params.CourseID, action.ID, ordinal, action.Unit,
			action.Payload); err != nil {
			return err
		}
	}
	if previous == nil || *previous == params.ContentID {
		return nil
	}
	_, err = n.db.Exec(ctx, navigationPrunePG, *previous)
	return err
}
