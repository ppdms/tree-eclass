package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// sqliteSynthesis implements database.Synthesis with native SQLite SQL: ?
// placeholders, unqualified table names, instr/JSON1 helpers and strftime
// clocks. No PostgreSQL compatibility functions remain.

// synthesisDueSQLite matches rows whose stored TEXT availability has passed.
// RFC3339Nano and legacy space-separated UTC stamps both normalize through
// julianday for the due comparison.
func synthesisDueSQLite(column string) string {
	return "julianday(" + column + ")<=julianday('now')"
}

// synthesisJSONTextSQLite reads a top-level JSON string field with a validity
// guard. json_valid runs before json_extract so malformed payloads return ”
// instead of raising.
func synthesisJSONTextSQLite(field string) string {
	return `(CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.` + field + `') ELSE '' END)`
}

func synthesisReadySourceSQLite() string {
	return `d.status='ready' AND d.content_hash_verified=1` +
		` AND e.status='ready' AND e.source_hash=d.source_hash` +
		` AND coalesce(e.requested_model,e.model)=?` +
		` AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN ? ELSE ? END` +
		` AND substr(ltrim(e.payload_json),1,1)='{'` +
		` AND length(CAST(e.payload_json AS BLOB))<=1048576` +
		` AND coalesce(` + synthesisJSONTextSQLite("summary") + `,'')<>''` +
		` AND coalesce(` + synthesisJSONTextSQLite("course_alignment") + `,'')<>'mismatch'`
}

func synthesisReadyOrderSQLite() string {
	return `CASE coalesce(` + synthesisJSONTextSQLite("importance") + `,'')` +
		` WHEN 'essential' THEN 0 WHEN 'useful' THEN 1 ELSE 2 END,d.id`
}

func (s sqliteSynthesis) AcquireExpensive(ctx context.Context) error {
	_, err := advisoryLock(ctx, s.db, "expensive-generation", true, false)
	return err
}

func (s sqliteSynthesis) RecoverLane(ctx context.Context, lane database.SynthesisLane, availableAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+` SET status='pending',claimed_at=NULL,`+
		`attempts=max(0,attempts-1),available_at=? WHERE status='running'`, availableAt)
	return err
}

func (s sqliteSynthesis) ScanDueCourse(ctx context.Context, lane database.SynthesisLane,
	config string) (database.SynthesisScan, error) {
	var out database.SynthesisScan
	err := s.db.QueryRow(ctx, `SELECT c.id FROM courses c JOIN course_exam_plans p ON p.course_id=c.id
 JOIN course_generation g ON g.course_id=c.id LEFT JOIN synthesis_scan s ON s.course_id=c.id AND s.lane=?
 WHERE p.enabled=1 AND p.exam_at IS NOT NULL AND p.exam_at<>'' AND p.commitment<>'skipped'
 AND (s.course_id IS NULL OR s.source_generation<>g.generation OR s.config_generation<>? OR `+synthesisDueSQLite("s.next_at")+`)
 ORDER BY s.next_at NULLS FIRST,c.id LIMIT 1`, lane.String(), config).Scan(&out.CourseID)
	return out, err
}

func (s sqliteSynthesis) RecordScan(ctx context.Context, lane database.SynthesisLane,
	course int64, config, nextAt string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO synthesis_scan(course_id,lane,source_generation,config_generation,next_at)
 SELECT course_id,?,generation,?,? FROM course_generation WHERE course_id=?
 ON CONFLICT(course_id,lane) DO UPDATE SET source_generation=excluded.source_generation,`+
		`config_generation=excluded.config_generation,next_at=excluded.next_at`, lane.String(), config, nextAt, course)
	return err
}

func (s sqliteSynthesis) ClaimDueRow(ctx context.Context, lane database.SynthesisLane,
	model, version string) (database.SynthesisClaim, error) {
	var out database.SynthesisClaim
	err := s.db.QueryRow(ctx, `SELECT id,course_id FROM `+synthesisTable(lane)+
		` WHERE status='pending' AND requested_model=? AND analysis_version=?`+
		` AND `+synthesisDueSQLite("available_at")+
		` ORDER BY priority DESC,julianday(available_at),id LIMIT 1`, model, version).Scan(&out.ID, &out.CourseID)
	return out, err
}

func (s sqliteSynthesis) LoadPacket(ctx context.Context, lane database.SynthesisLane,
	id int64) (database.SynthesisPacket, error) {
	var out database.SynthesisPacket
	fields := `revision_hash,'' unit_key,'' blueprint_revision_hash`
	if lane == database.SynthesisPractice {
		fields = `set_hash,unit_key,blueprint_revision_hash`
	}
	var raw *string
	err := s.db.QueryRow(ctx, `SELECT `+fields+`,attempts,`+
		`CASE WHEN length(CAST(evidence_packet_json AS BLOB))<=2097152 THEN evidence_packet_json END`+
		` FROM `+synthesisTable(lane)+` WHERE id=? AND status='pending'`+
		` AND `+synthesisDueSQLite("available_at"), id).
		Scan(&out.Hash, &out.UnitKey, &out.Blueprint, &out.Attempts, &raw)
	if err != nil {
		return database.SynthesisPacket{}, err
	}
	if raw == nil {
		return database.SynthesisPacket{}, database.ErrPacketTooLarge
	}
	out.PacketJSON = *raw
	return out, nil
}

func (s sqliteSynthesis) MarkClaimed(ctx context.Context, lane database.SynthesisLane,
	id int64, claimedAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+
		` SET status='running',claimed_at=?,attempts=attempts+1,error=NULL WHERE id=?`, claimedAt, id)
	return err
}

func (s sqliteSynthesis) AbandonClaim(ctx context.Context, lane database.SynthesisLane,
	id int64, finishedAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+
		` SET status='stale',finished_at=? WHERE id=?`, finishedAt, id)
	return err
}

func (s sqliteSynthesis) EvidenceCounts(ctx context.Context,
	params database.SynthesisEvidenceParams) (database.SynthesisEvidence, error) {
	var out database.SynthesisEvidence
	from := ` FROM documents d LEFT JOIN document_enrichments e ON e.document_id=d.id` +
		` WHERE d.course_id=? AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped')` +
		` AND ` + documentAdmissionSQLite
	err := s.db.QueryRow(ctx, `SELECT count(*),count(CASE WHEN `+synthesisReadySourceSQLite()+` THEN 1 END)`+from,
		params.Model, params.PageVersion, params.DocVersion, params.CourseID).Scan(&out.Total, &out.Ready)
	return out, err
}

func (s sqliteSynthesis) EvidenceDocuments(ctx context.Context,
	params database.SynthesisEvidenceParams) ([]database.SynthesisDocument, error) {
	from := ` FROM documents d LEFT JOIN document_enrichments e ON e.document_id=d.id` +
		` WHERE d.course_id=? AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped')` +
		` AND ` + documentAdmissionSQLite
	rows, err := s.db.Query(ctx, `SELECT d.id,d.source_hash,d.display_name,d.source_path,d.source_origin,d.document_kind,`+
		`e.source_hash,e.analysis_version,e.model,coalesce(e.requested_model,e.model),e.context_hash,e.payload_json`+
		from+` AND `+synthesisReadySourceSQLite()+` ORDER BY `+synthesisReadyOrderSQLite()+` LIMIT ?`,
		params.CourseID, params.Model, params.PageVersion, params.DocVersion, params.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SynthesisDocument{}
	for rows.Next() {
		var doc database.SynthesisDocument
		if err := rows.Scan(&doc.ID, &doc.SourceHash, &doc.DisplayName, &doc.SourcePath, &doc.SourceOrigin,
			&doc.DocumentKind, &doc.AnalysisHash, &doc.Version, &doc.Model, &doc.Requested,
			&doc.ContextHash, &doc.Payload); err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, rows.Err()
}

func (s sqliteSynthesis) DocumentExcerpts(ctx context.Context, document string) ([]database.SynthesisExcerpt, error) {
	rows, err := s.db.Query(ctx, `SELECT ordinal,locator_type,locator_start,substr(text,1,1200)`+
		` FROM chunks WHERE document_id=? ORDER BY ordinal`, document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SynthesisExcerpt{}
	for rows.Next() {
		var item database.SynthesisExcerpt
		if err := rows.Scan(&item.Ordinal, &item.Type, &item.Start, &item.Text); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
