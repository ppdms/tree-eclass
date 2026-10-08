package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// postgresSynthesis community, blueprint, queue and publish operations.
func (s postgresSynthesis) SearchCommunityIDs(ctx context.Context,
	params database.SynthesisCommunityParams) ([]string, error) {
	query := `SELECT c.conversation_id,c.text,c.channel_name FROM messages.conversations c` +
		` JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text` +
		` AND mapping.course_id=c.course_id` +
		` JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id` +
		` AND a.root_id=c.root_id::text AND a.status='ready'` +
		` JOIN messages.conversations_fts f USING(conversation_id)` +
		` WHERE c.course_id=$1 ORDER BY c.ended_at_epoch DESC,c.conversation_id COLLATE "C"`
	return synthesisCommunityIDs(ctx, s.db, query, params)
}

func (s postgresSynthesis) CommunityEntry(ctx context.Context,
	course int64, id string) (database.SynthesisCommunityEntry, error) {
	var out database.SynthesisCommunityEntry
	err := s.db.QueryRow(ctx, `SELECT c.channel_name,c.ended_at,c.channel_id::text,ch.guild_id`+
		` FROM messages.conversations c LEFT JOIN messages.channels ch`+
		` ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id`+
		` WHERE c.conversation_id=$1 AND c.course_id=$2`, id, course).
		Scan(&out.ChannelName, &out.EndedAt, &out.ChannelID, &out.GuildID)
	return out, err
}

func (s postgresSynthesis) CommunityMessages(ctx context.Context,
	course int64, id string) ([]database.SynthesisCommunityMessage, error) {
	rows, err := s.db.Query(ctx, `SELECT m.message_id::text,m.timestamp,`+
		`substr(m.author_name,1,100),substr(m.content,1,500)`+
		` FROM messages.conversation_messages cm JOIN messages.messages m USING(source_path,message_id)`+
		` WHERE cm.conversation_id=$1 AND m.course_id=$2 ORDER BY cm.position,m.message_id LIMIT 8`, id, course)
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

func (s postgresSynthesis) ReadyBlueprint(ctx context.Context, course int64,
	model, version string) (database.SynthesisBlueprint, error) {
	var out database.SynthesisBlueprint
	err := s.db.QueryRow(ctx, `SELECT revision_hash,`+
		`CASE WHEN octet_length(payload_json)<=1048576 THEN payload_json END,`+
		`CASE WHEN octet_length(evidence_packet_json)<=2097152 THEN evidence_packet_json END`+
		` FROM knowledge.course_blueprints WHERE course_id=$1 AND status='ready'`+
		` AND requested_model=$2 AND analysis_version=$3`, course, model, version).
		Scan(&out.Revision, &out.Payload, &out.Packet)
	return out, err
}

func (s postgresSynthesis) BlueprintReady(ctx context.Context, course int64,
	revision, model, version string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.course_blueprints`+
		` WHERE course_id=$1 AND revision_hash=$2 AND status='ready' AND requested_model=$3 AND analysis_version=$4)`,
		course, revision, model, version).Scan(&exists)
	return exists, err
}

func (s postgresSynthesis) QueueCourseRevision(ctx context.Context, params database.QueueCourseParams) error {
	if _, err := s.db.Exec(ctx, `UPDATE knowledge.course_blueprints SET status='stale',finished_at=$2,claimed_at=NULL`+
		` WHERE course_id=$1 AND revision_hash<>$3 AND status IN('pending','running')`,
		params.CourseID, params.AvailableAt, params.RevisionHash); err != nil {
		return err
	}
	var status string
	err := s.db.QueryRow(ctx, `SELECT status FROM knowledge.course_blueprints WHERE revision_hash=$1`,
		params.RevisionHash).Scan(&status)
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = s.db.Exec(ctx, `UPDATE knowledge.course_blueprints SET status='pending',`+
			`revision=(SELECT max(revision)+1 FROM knowledge.course_blueprints WHERE course_id=$1),`+
			`attempts=0,claimed_at=NULL,error=NULL,available_at=$3,created_at=$3,finished_at=NULL,`+
			`payload_json=NULL,generated_at=NULL WHERE revision_hash=$2`,
			params.CourseID, params.RevisionHash, params.AvailableAt)
		return err
	}
	if !database.IsNoRows(err) {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO knowledge.course_blueprints(course_id,revision,revision_hash,evidence_hash,`+
		`evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)`+
		` SELECT $1,coalesce(max(revision),0)+1,$2,$3,$4,$5,$6,$6,$7,$7 FROM knowledge.course_blueprints WHERE course_id=$1`,
		params.CourseID, params.RevisionHash, params.EvidenceHash, params.PacketJSON,
		params.Version, params.Model, params.AvailableAt)
	return err
}

func (s postgresSynthesis) QueuePracticeRevision(ctx context.Context, params database.QueuePracticeParams) error {
	if _, err := s.db.Exec(ctx, `UPDATE knowledge.practice_question_sets SET status='stale',claimed_at=NULL,finished_at=$3`+
		` WHERE course_id=$1 AND unit_key=$2 AND set_hash<>$4 AND status IN('pending','running')`,
		params.CourseID, params.UnitKey, params.AvailableAt, params.SetHash); err != nil {
		return err
	}
	var status string
	err := s.db.QueryRow(ctx, `SELECT status FROM knowledge.practice_question_sets WHERE set_hash=$1`,
		params.SetHash).Scan(&status)
	if err == nil {
		if status != "stale" {
			return nil
		}
		_, err = s.db.Exec(ctx, `UPDATE knowledge.practice_question_sets SET status='pending',attempts=0,`+
			`claimed_at=NULL,error=NULL,available_at=$2,finished_at=NULL,payload_json=NULL,generated_at=NULL WHERE set_hash=$1`,
			params.SetHash, params.AvailableAt)
		return err
	}
	if !database.IsNoRows(err) {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO knowledge.practice_question_sets(course_id,unit_key,set_hash,`+
		`blueprint_revision_hash,evidence_hash,evidence_packet_json,analysis_version,requested_model,model,available_at,created_at)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$9)`,
		params.CourseID, params.UnitKey, params.SetHash, params.Revision,
		params.EvidenceHash, params.PacketJSON, params.Version, params.Model, params.AvailableAt)
	return err
}

func (s postgresSynthesis) LockCourseForPublish(ctx context.Context, course int64) error {
	var locked int64
	return s.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, course).Scan(&locked)
}

func (s postgresSynthesis) LockGenerationForPublish(ctx context.Context, course int64) error {
	var generation int64
	return s.db.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=$1 FOR UPDATE`,
		course).Scan(&generation)
}

func (s postgresSynthesis) ClaimActive(ctx context.Context, lane database.SynthesisLane,
	id int64, claimedAt string) (bool, error) {
	var active bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+synthesisTablePG(lane)+
		` WHERE id=$1 AND claimed_at=$2 AND status='running')`, id, claimedAt).Scan(&active)
	return active, err
}

func (s postgresSynthesis) PublishReady(ctx context.Context, lane database.SynthesisLane,
	params database.PublishParams) error {
	scope := `course_id=$1`
	args := []any{params.CourseID, params.ReadyAt}
	if lane == database.SynthesisPractice {
		scope += ` AND unit_key=$3`
		args = append(args, params.UnitKey)
	}
	if _, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+
		` SET status='stale',finished_at=$2 WHERE `+scope+` AND status='ready'`, args...); err != nil {
		return err
	}
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+
		` SET status='ready',model=$3,payload_json=$4,generated_at=$5,finished_at=$5,claimed_at=NULL,error=NULL`+
		` WHERE id=$1 AND claimed_at=$2 AND status='running'`,
		params.ID, params.ClaimedAt, params.Model, params.Payload, params.ReadyAt)
	return err
}

func (s postgresSynthesis) ReplaceQuestions(ctx context.Context,
	course int64, setID int64, unit string, questions []database.SynthesisQuestion) error {
	if _, err := s.db.Exec(ctx, `DELETE FROM knowledge.practice_questions WHERE set_id=$1`, setID); err != nil {
		return err
	}
	for i, q := range questions {
		if _, err := s.db.Exec(ctx, `INSERT INTO knowledge.practice_questions(question_id,set_id,course_id,`+
			`unit_key,ordinal,question_key,response_mode,difficulty,estimated_minutes,payload_json)`+
			` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			q.ID, setID, course, unit, i+1, q.Key, q.Mode, q.Difficulty, q.Minutes, q.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (s postgresSynthesis) FinishClaim(ctx context.Context, lane database.SynthesisLane,
	params database.FinishParams) error {
	_, err := s.db.Exec(ctx, `UPDATE `+synthesisTablePG(lane)+
		` SET status=$3,error=$4,available_at=$5,claimed_at=NULL,`+
		`attempts=CASE WHEN $6 THEN greatest(0,attempts-1) ELSE attempts END,`+
		`finished_at=CASE WHEN $3 IN('stale','failed') THEN $7 ELSE NULL END`+
		` WHERE id=$1 AND claimed_at=$2 AND status='running'`,
		params.ID, params.ClaimedAt, params.Status, params.Error, params.AvailableAt, params.Reset, params.FinishedAt)
	return err
}
