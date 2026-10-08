package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// postgresSynthesis implements database.Synthesis with native PostgreSQL SQL.
func (s postgresSynthesis) AcquireExpensive(ctx context.Context) error {
	_, err := advisoryLock(ctx, s.db, "expensive-generation", true, false)
	return err
}

func (s postgresSynthesis) RecoverLane(ctx context.Context, lane database.SynthesisLane, availableAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+` SET status='pending',claimed_at=NULL,`+
		`attempts=greatest(0,attempts-1),available_at=$1 WHERE status='running'`, availableAt)
	return err
}

func (s postgresSynthesis) ScanDueCourse(ctx context.Context, lane database.SynthesisLane,
	config string) (database.SynthesisScan, error) {
	var out database.SynthesisScan
	err := s.db.QueryRow(ctx, `SELECT c.id FROM app.courses c JOIN app.course_exam_plans p ON p.course_id=c.id
 JOIN read_model.course_generation g ON g.course_id=c.id LEFT JOIN read_model.synthesis_scan s ON s.course_id=c.id AND s.lane=$1
 WHERE p.enabled=1 AND p.exam_at IS NOT NULL AND p.exam_at<>'' AND p.commitment<>'skipped'
 AND (s.course_id IS NULL OR s.source_generation<>g.generation OR s.config_generation<>$2 OR s.next_at<=now())
 ORDER BY s.next_at NULLS FIRST,c.id LIMIT 1 FOR UPDATE OF c SKIP LOCKED`, lane.String(), config).Scan(&out.CourseID)
	return out, err
}

func (s postgresSynthesis) RecordScan(ctx context.Context, lane database.SynthesisLane,
	course int64, config, nextAt string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO read_model.synthesis_scan(course_id,lane,source_generation,config_generation,next_at)
 SELECT course_id,$2,generation,$3,$4 FROM read_model.course_generation WHERE course_id=$1
 ON CONFLICT(course_id,lane) DO UPDATE SET source_generation=excluded.source_generation,`+
		`config_generation=excluded.config_generation,next_at=excluded.next_at`, course, lane.String(), config, nextAt)
	return err
}

func (s postgresSynthesis) ClaimDueRow(ctx context.Context, lane database.SynthesisLane,
	model, version string) (database.SynthesisClaim, error) {
	var out database.SynthesisClaim
	err := s.db.QueryRow(ctx, `SELECT id,course_id FROM `+synthesisTablePG(lane)+
		` WHERE status='pending' AND requested_model=$1 AND analysis_version=$2`+
		` AND available_at::timestamptz<=clock_timestamp()`+
		` ORDER BY priority DESC,available_at::timestamptz,id LIMIT 1`, model, version).Scan(&out.ID, &out.CourseID)
	return out, err
}

func (s postgresSynthesis) LoadPacket(ctx context.Context, lane database.SynthesisLane,
	id int64) (database.SynthesisPacket, error) {
	var out database.SynthesisPacket
	fields := `revision_hash,'' unit_key,'' blueprint_revision_hash`
	if lane == database.SynthesisPractice {
		fields = `set_hash,unit_key,blueprint_revision_hash`
	}
	var raw *string
	err := s.db.QueryRow(ctx, `SELECT `+fields+`,attempts,`+
		`CASE WHEN octet_length(evidence_packet_json)<=2097152 THEN evidence_packet_json END`+
		` FROM `+synthesisTablePG(lane)+` WHERE id=$1 AND status='pending'`+
		` AND available_at::timestamptz<=clock_timestamp() FOR UPDATE`, id).
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

func (s postgresSynthesis) MarkClaimed(ctx context.Context, lane database.SynthesisLane,
	id int64, claimedAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+
		` SET status='running',claimed_at=$2,attempts=attempts+1,error=NULL WHERE id=$1`, id, claimedAt)
	return err
}

func (s postgresSynthesis) AbandonClaim(ctx context.Context, lane database.SynthesisLane,
	id int64, finishedAt string) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+
		` SET status='stale',finished_at=$2 WHERE id=$1`, id, finishedAt)
	return err
}

func (s postgresSynthesis) EvidenceCounts(ctx context.Context,
	params database.SynthesisEvidenceParams) (database.SynthesisEvidence, error) {
	var out database.SynthesisEvidence
	from := ` FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id` +
		` WHERE d.course_id=$1 AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped')` +
		` AND ` + documentAdmissionPG
	err := s.db.QueryRow(ctx, `SELECT count(*),count(CASE WHEN `+synthesisReadySourcePG(2)+` THEN 1 END)`+from,
		params.CourseID, params.Model, params.DocVersion, params.PageVersion).Scan(&out.Total, &out.Ready)
	return out, err
}

func (s postgresSynthesis) EvidenceDocuments(ctx context.Context,
	params database.SynthesisEvidenceParams) ([]database.SynthesisDocument, error) {
	from := ` FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id` +
		` WHERE d.course_id=$1 AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped')` +
		` AND ` + documentAdmissionPG
	rows, err := s.db.Query(ctx, `SELECT d.id,d.source_hash,d.display_name,d.source_path,d.source_origin,d.document_kind,`+
		`e.source_hash,e.analysis_version,e.model,coalesce(e.requested_model,e.model),e.context_hash,e.payload_json`+
		from+` AND `+synthesisReadySourcePG(2)+` ORDER BY `+synthesisReadyOrderPG()+` LIMIT $5`,
		params.CourseID, params.Model, params.DocVersion, params.PageVersion, params.Limit)
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

func (s postgresSynthesis) DocumentExcerpts(ctx context.Context, document string) ([]database.SynthesisExcerpt, error) {
	rows, err := s.db.Query(ctx, `SELECT ordinal,locator_type,locator_start,left(text,1200)`+
		` FROM knowledge.chunks WHERE document_id=$1 ORDER BY ordinal`, document)
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
