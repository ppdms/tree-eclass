package rdbms

import (
	"context"
	"fmt"
	"strings"

	"tree-eclass/internal/domain/database"
)

type postgresPractice struct{ db nativeDBTX }

// practicePlaceholders renders $1..$n for a native IN list starting at offset.
func practicePlaceholders(offset, count int) string {
	items := make([]string, 0, count)
	for i := range count {
		items = append(items, fmt.Sprintf("$%d", offset+i))
	}
	return strings.Join(items, ",")
}

func (p postgresPractice) LockCourse(ctx context.Context, course int64) error {
	var id int64
	err := p.db.QueryRow(ctx, `SELECT id FROM app.courses
		WHERE id=$1 AND (hidden=0 OR EXISTS(
			SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=app.courses.id AND p.enabled=1))
		FOR SHARE`, course).Scan(&id)
	return err
}

func (p postgresPractice) LockedGeneration(ctx context.Context, course int64) (int64, error) {
	var generation int64
	err := p.db.QueryRow(ctx, `SELECT generation FROM read_model.course_generation
		WHERE course_id=$1 FOR SHARE`, course).Scan(&generation)
	return generation, err
}

func (p postgresPractice) ListQuestionSets(
	ctx context.Context, selector database.PracticeSetSelector,
) ([]database.PracticeQuestionSet, error) {
	rows, err := p.db.Query(ctx, `SELECT DISTINCT ON(unit_key)
			id,unit_key,set_hash,status,blueprint_revision_hash,model,generated_at
		FROM knowledge.practice_question_sets
		WHERE course_id=$1 AND blueprint_revision_hash=$2 AND analysis_version=$3
			AND requested_model=$4 AND ($5='' OR unit_key=$5)
			AND status IN('ready','pending','running','failed')
		ORDER BY unit_key,(status='ready') DESC,id DESC LIMIT 201`,
		selector.CourseID, selector.Revision, selector.AnalysisVersion, selector.Model, selector.UnitKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.PracticeQuestionSet{}
	for rows.Next() {
		var set database.PracticeQuestionSet
		set.CourseID = selector.CourseID
		if err := rows.Scan(
			&set.ID, &set.UnitKey, &set.SetHash, &set.Status, &set.Revision, &set.Model, &set.Generated,
		); err != nil {
			return nil, err
		}
		out = append(out, set)
	}
	return out, rows.Err()
}

func (p postgresPractice) ListQuestions(ctx context.Context, setIDs []int64) ([]database.PracticeQuestionRow, error) {
	out := []database.PracticeQuestionRow{}
	if len(setIDs) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(setIDs))
	for _, id := range setIDs {
		args = append(args, id)
	}
	rows, err := p.db.Query(ctx, `SELECT set_id,question_id,course_id,unit_key,question_key,
			CASE WHEN octet_length(payload_json)<=65536 THEN payload_json END
		FROM knowledge.practice_questions
		WHERE set_id IN(`+practicePlaceholders(1, len(args))+`)
		ORDER BY set_id,ordinal LIMIT 2401`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row database.PracticeQuestionRow
		if err := rows.Scan(&row.SetID, &row.Question, &row.CourseID, &row.UnitKey, &row.Key, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (p postgresPractice) QuestionProgress(
	ctx context.Context, course int64, questions []string,
) ([]database.PracticeProgress, error) {
	out := []database.PracticeProgress{}
	if len(questions) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(questions)+1)
	args = append(args, course)
	for _, id := range questions {
		args = append(args, id)
	}
	rows, err := p.db.Query(ctx, `WITH history AS (
			SELECT question_id,outcome,attempted_at,
				row_number() OVER(PARTITION BY question_id ORDER BY attempted_at DESC,id DESC) rn
			FROM app.practice_attempts
			WHERE course_id=$1 AND question_id IN(`+practicePlaceholders(2, len(questions))+`)
		) SELECT question_id,count(*),
			coalesce(min(rn) FILTER(WHERE outcome<>'correct'),count(*)+1)-1,
			max(outcome) FILTER(WHERE rn=1),max(attempted_at) FILTER(WHERE rn=1)
		FROM history GROUP BY question_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row database.PracticeProgress
		if err := rows.Scan(&row.Question, &row.Attempts, &row.Streak, &row.Last, &row.AttemptedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (p postgresPractice) AdmissionQuestion(
	ctx context.Context, selector database.PracticeAdmissionSelector,
) (database.PracticeAdmission, error) {
	var row database.PracticeAdmission
	err := p.db.QueryRow(ctx, `SELECT q.unit_key,q.question_key,s.set_hash,s.blueprint_revision_hash,
			CASE WHEN octet_length(q.payload_json)<=65536 THEN q.payload_json END,c.payload
		FROM read_model.navigation n
		JOIN read_model.roadmap_content c ON c.content_id=n.content_id
		JOIN knowledge.practice_question_sets s
			ON s.course_id=n.course_id AND s.blueprint_revision_hash=n.revision_id
		JOIN knowledge.practice_questions q
			ON q.set_id=s.id AND q.course_id=s.course_id AND q.unit_key=s.unit_key
		WHERE n.course_id=$1 AND n.source_generation=$2 AND n.config_generation=$3
			AND n.overview->>'usable'='true'
			AND s.status='ready' AND s.analysis_version=$4 AND s.requested_model=$5
			AND q.question_id=$6
		FOR SHARE OF n,s,q`,
		selector.CourseID, selector.Generation, selector.Config,
		selector.AnalysisVersion, selector.Model, selector.Question,
	).Scan(&row.Unit, &row.Key, &row.Set, &row.Revision, &row.Payload, &row.Evidence)
	return row, err
}

func (p postgresPractice) AttemptByKey(ctx context.Context, key string) (database.PracticeAttemptRow, error) {
	var row database.PracticeAttemptRow
	err := p.db.QueryRow(ctx, `SELECT id,course_id,unit_key,question_id,outcome,idempotency_key,
			confidence,seconds,answer,note,study_event_id,question_key,set_hash,blueprint_revision_hash
		FROM app.practice_attempts WHERE idempotency_key=$1`, key,
	).Scan(
		&row.ID, &row.CourseID, &row.Unit, &row.Question, &row.Outcome, &row.Key,
		&row.Confidence, &row.Seconds, &row.Answer, &row.Note, &row.EventID,
		&row.QuestionKey, &row.Set, &row.Revision,
	)
	return row, err
}

func (p postgresPractice) InsertAttempt(
	ctx context.Context, params database.PracticeAttemptParams,
) (int64, int64, error) {
	var eventID, attemptID int64
	err := p.db.QueryRow(ctx, `INSERT INTO app.study_unit_events
			(course_id,plan_revision,action_id,unit_key,event_type,idempotency_key,confidence,score,note)
		VALUES($1,$2,$3,$4,'recall_answered',$5,$6,$7,$8) RETURNING id`,
		params.CourseID, params.Revision, params.Question, params.Unit,
		params.EventKey, params.Confidence, params.Score, params.Note,
	).Scan(&eventID)
	if err != nil {
		return 0, 0, err
	}
	err = p.db.QueryRow(ctx, `INSERT INTO app.practice_attempts
			(course_id,unit_key,question_id,question_key,set_hash,blueprint_revision_hash,
			outcome,grading_mode,confidence,seconds,answer,note,idempotency_key,study_event_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,'self',$8,$9,$10,$11,$12,$13) RETURNING id`,
		params.CourseID, params.Unit, params.Question, params.Key, params.Set, params.Revision,
		params.Outcome, params.Confidence, params.Seconds, params.Answer, params.Note,
		params.IdemKey, eventID,
	).Scan(&attemptID)
	if err != nil {
		return 0, 0, err
	}
	return attemptID, eventID, nil
}

func (p postgresPractice) SetStatusCounts(
	ctx context.Context, selector database.PracticeSetSelector, units []string,
) ([]database.PracticeSetStatusCount, error) {
	out := []database.PracticeSetStatusCount{}
	if len(units) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(units)+4)
	args = append(args, selector.CourseID, selector.Revision, selector.AnalysisVersion, selector.Model)
	for _, unit := range units {
		args = append(args, unit)
	}
	rows, err := p.db.Query(ctx, `WITH chosen AS (
			SELECT DISTINCT ON(unit_key) id,status FROM knowledge.practice_question_sets
			WHERE course_id=$1 AND blueprint_revision_hash=$2
				AND analysis_version=$3 AND requested_model=$4
				AND unit_key IN(`+practicePlaceholders(5, len(units))+`)
				AND status IN('ready','pending','running','failed')
			ORDER BY unit_key,(status='ready') DESC,id DESC
		) SELECT status,count(*),
			coalesce(sum((SELECT count(*) FROM knowledge.practice_questions q WHERE q.set_id=s.id)),0)::bigint
		FROM chosen s GROUP BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row database.PracticeSetStatusCount
		if err := rows.Scan(&row.Status, &row.Sets, &row.Questions); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
