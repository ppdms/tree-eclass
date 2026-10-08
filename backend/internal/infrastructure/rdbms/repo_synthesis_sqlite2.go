package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// sqliteSynthesis community, blueprint, queue and publish operations.
func (s sqliteSynthesis) SearchCommunityIDs(ctx context.Context,
	params database.SynthesisCommunityParams) ([]string, error) {
	query := `SELECT c.conversation_id,c.text,c.channel_name FROM conversations c` +
		` JOIN discord_course_channels mapping ON mapping.root_channel_id=CAST(c.root_id AS TEXT)` +
		` AND mapping.course_id=c.course_id` +
		` JOIN archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id` +
		` AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'` +
		` JOIN conversations_fts f USING(conversation_id)` +
		` WHERE c.course_id=? ORDER BY c.ended_at_epoch DESC,c.conversation_id`
	return synthesisCommunityIDs(ctx, s.db, query, params)
}

func (s sqliteSynthesis) CommunityEntry(ctx context.Context,
	course int64, id string) (database.SynthesisCommunityEntry, error) {
	var out database.SynthesisCommunityEntry
	err := s.db.QueryRow(ctx, `SELECT c.channel_name,c.ended_at,CAST(c.channel_id AS TEXT),ch.guild_id`+
		` FROM conversations c LEFT JOIN channels ch`+
		` ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id`+
		` WHERE c.conversation_id=? AND c.course_id=?`, id, course).
		Scan(&out.ChannelName, &out.EndedAt, &out.ChannelID, &out.GuildID)
	return out, err
}

func (s sqliteSynthesis) CommunityMessages(ctx context.Context,
	course int64, id string) ([]database.SynthesisCommunityMessage, error) {
	rows, err := s.db.Query(ctx, `SELECT CAST(m.message_id AS TEXT),m.timestamp,`+
		`substr(m.author_name,1,100),substr(m.content,1,500)`+
		` FROM conversation_messages cm JOIN messages m USING(source_path,message_id)`+
		` WHERE cm.conversation_id=? AND m.course_id=? ORDER BY cm.position,m.message_id LIMIT 8`, id, course)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SynthesisCommunityMessage{}
	for rows.Next() {
		var item database.SynthesisCommunityMessage
		if err := rows.Scan(&item.MessageID, &item.Timestamp, &item.Author, &item.Content); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s sqliteSynthesis) ReadyBlueprint(ctx context.Context, course int64,
	model, version string) (database.SynthesisBlueprint, error) {
	var out database.SynthesisBlueprint
	err := s.db.QueryRow(ctx, `SELECT revision_hash,`+
		`CASE WHEN length(CAST(payload_json AS BLOB))<=1048576 THEN payload_json END,`+
		`CASE WHEN length(CAST(evidence_packet_json AS BLOB))<=2097152 THEN evidence_packet_json END`+
		` FROM course_blueprints WHERE course_id=? AND status='ready'`+
		` AND requested_model=? AND analysis_version=?`, course, model, version).
		Scan(&out.Revision, &out.Payload, &out.Packet)
	return out, err
}

func (s sqliteSynthesis) BlueprintReady(ctx context.Context, course int64,
	revision, model, version string) (bool, error) {
	var exists int64
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM course_blueprints`+
		` WHERE course_id=? AND revision_hash=? AND status='ready' AND requested_model=? AND analysis_version=?)`,
		course, revision, model, version).Scan(&exists)
	return exists != 0, err
}

func (s sqliteSynthesis) QueueCourseRevision(ctx context.Context, params database.QueueCourseParams) error {
	if _, err := s.db.Exec(ctx, `UPDATE course_blueprints SET status='stale',finished_at=?,claimed_at=NULL`+
		` WHERE course_id=? AND revision_hash<>? AND status IN('pending','running')`,
		params.AvailableAt, params.CourseID, params.RevisionHash); err != nil {
		return err
	}
	var status string
	err := s.db.QueryRow(ctx, `SELECT status FROM course_blueprints WHERE revision_hash=?`,
		params.RevisionHash).Scan(&status)
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = s.db.Exec(ctx, `UPDATE course_blueprints SET status='pending',`+
			`revision=(SELECT max(revision)+1 FROM course_blueprints WHERE course_id=?),`+
			`attempts=0,claimed_at=NULL,error=NULL,available_at=?,created_at=?,finished_at=NULL,`+
			`payload_json=NULL,generated_at=NULL WHERE revision_hash=?`,
			params.CourseID, params.AvailableAt, params.AvailableAt, params.RevisionHash)
		return err
	}
	if !database.IsNoRows(err) {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO course_blueprints(course_id,revision,revision_hash,evidence_hash,`+
		`evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)`+
		` SELECT ?,coalesce(max(revision),0)+1,?,?,?,?,?,?,?,? FROM course_blueprints WHERE course_id=?`,
		params.CourseID, params.RevisionHash, params.EvidenceHash, params.PacketJSON,
		params.Version, params.Model, params.Model, params.AvailableAt, params.AvailableAt, params.CourseID)
	return err
}

func (s sqliteSynthesis) QueuePracticeRevision(ctx context.Context, params database.QueuePracticeParams) error {
	if _, err := s.db.Exec(ctx, `UPDATE practice_question_sets SET status='stale',claimed_at=NULL,finished_at=?`+
		` WHERE course_id=? AND unit_key=? AND set_hash<>? AND status IN('pending','running')`,
		params.AvailableAt, params.CourseID, params.UnitKey, params.SetHash); err != nil {
		return err
	}
	var status string
	err := s.db.QueryRow(ctx, `SELECT status FROM practice_question_sets WHERE set_hash=?`,
		params.SetHash).Scan(&status)
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = s.db.Exec(ctx, `UPDATE practice_question_sets SET status='pending',attempts=0,`+
			`claimed_at=NULL,error=NULL,available_at=?,finished_at=NULL,payload_json=NULL,generated_at=NULL WHERE set_hash=?`,
			params.AvailableAt, params.SetHash)
		return err
	}
	if !database.IsNoRows(err) {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO practice_question_sets(course_id,unit_key,set_hash,`+
		`blueprint_revision_hash,evidence_hash,evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		params.CourseID, params.UnitKey, params.SetHash, params.Revision,
		params.EvidenceHash, params.PacketJSON, params.Version, params.Model, params.Model,
		params.AvailableAt, params.AvailableAt)
	return err
}

func (s sqliteSynthesis) LockCourseForPublish(ctx context.Context, course int64) error {
	var locked int64
	return s.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`, course).Scan(&locked)
}

func (s sqliteSynthesis) LockGenerationForPublish(ctx context.Context, course int64) error {
	var generation int64
	return s.db.QueryRow(ctx, `SELECT generation FROM course_generation WHERE course_id=?`, course).Scan(&generation)
}

func (s sqliteSynthesis) ClaimActive(ctx context.Context, lane database.SynthesisLane,
	id int64, claimedAt string) (bool, error) {
	var active int64
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+synthesisTable(lane)+
		` WHERE id=? AND claimed_at=? AND status='running')`, id, claimedAt).Scan(&active)
	return active != 0, err
}

func (s sqliteSynthesis) PublishReady(ctx context.Context, lane database.SynthesisLane,
	params database.PublishParams) error {
	scope := `course_id=?`
	args := []any{params.ReadyAt, params.CourseID}
	if lane == database.SynthesisPractice {
		scope += ` AND unit_key=?`
		args = append(args, params.UnitKey)
	}
	if _, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+
		` SET status='stale',finished_at=? WHERE `+scope+` AND status='ready'`, args...); err != nil {
		return err
	}
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+
		` SET status='ready',model=?,payload_json=?,generated_at=?,finished_at=?,claimed_at=NULL,error=NULL`+
		` WHERE id=? AND claimed_at=? AND status='running'`,
		params.Model, params.Payload, params.ReadyAt, params.ReadyAt, params.ID, params.ClaimedAt)
	return err
}

func (s sqliteSynthesis) ReplaceQuestions(ctx context.Context,
	course int64, setID int64, unit string, questions []database.SynthesisQuestion) error {
	if _, err := s.db.Exec(ctx, `DELETE FROM practice_questions WHERE set_id=?`, setID); err != nil {
		return err
	}
	for i, q := range questions {
		if _, err := s.db.Exec(ctx, `INSERT INTO practice_questions(question_id,set_id,course_id,`+
			`unit_key,ordinal,question_key,response_mode,difficulty,estimated_minutes,payload_json)`+
			` VALUES(?,?,?,?,?,?,?,?,?,?)`,
			q.ID, setID, course, unit, i+1, q.Key, q.Mode, q.Difficulty, q.Minutes, q.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (s sqliteSynthesis) FinishClaim(ctx context.Context, lane database.SynthesisLane,
	params database.FinishParams) error {
	reset := int64(0)
	if params.Reset {
		reset = 1
	}
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTable(lane)+
		` SET status=?,error=?,available_at=?,claimed_at=NULL,`+
		`attempts=CASE WHEN ?<>0 THEN max(0,attempts-1) ELSE attempts END,`+
		`finished_at=CASE WHEN ? IN('stale','failed') THEN ? ELSE NULL END`+
		` WHERE id=? AND claimed_at=? AND status='running'`,
		params.Status, params.Error, params.AvailableAt, reset,
		params.Status, params.FinishedAt, params.ID, params.ClaimedAt)
	return err
}
