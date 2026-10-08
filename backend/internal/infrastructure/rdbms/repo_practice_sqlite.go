package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
)

type sqlitePractice struct{ db nativeDBTX }

// practiceSQLitePlaceholders renders ?,?,... for a native IN list.
func practiceSQLitePlaceholders(count int) string {
	if count <= 0 {
		return "NULL"
	}
	return strings.Repeat("?,", count-1) + "?"
}

func (p sqlitePractice) LockCourse(ctx context.Context, course int64) error {
	// The admitted writer transaction serializes admission; SQLite takes no
	// row locks. The visibility check matches the PostgreSQL predicate.
	var id int64
	err := p.db.QueryRow(ctx, `SELECT id FROM courses
		WHERE id=? AND (hidden=0 OR EXISTS(
			SELECT 1 FROM course_exam_plans p WHERE p.course_id=courses.id AND p.enabled=1))`, course).Scan(&id)
	return err
}

func (p sqlitePractice) LockedGeneration(ctx context.Context, course int64) (int64, error) {
	var generation int64
	err := p.db.QueryRow(ctx, `SELECT generation FROM course_generation WHERE course_id=?`, course).Scan(&generation)
	return generation, err
}

func (p sqlitePractice) ListQuestionSets(
	ctx context.Context, selector database.PracticeSetSelector,
) ([]database.PracticeQuestionSet, error) {
	// The DISTINCT ON preference (ready first, then newest per unit) is a
	// direct window-function selection; tie order matches PostgreSQL.
	rows, err := p.db.Query(ctx, `SELECT id,unit_key,set_hash,status,blueprint_revision_hash,model,generated_at
		FROM (
			SELECT id,unit_key,set_hash,status,blueprint_revision_hash,model,generated_at,
				ROW_NUMBER() OVER(
					PARTITION BY unit_key ORDER BY (status='ready') DESC,id DESC) rn
			FROM practice_question_sets
			WHERE course_id=? AND blueprint_revision_hash=? AND analysis_version=?
				AND requested_model=? AND (?='' OR unit_key=?)
				AND status IN('ready','pending','running','failed')
		) WHERE rn=1 ORDER BY unit_key LIMIT 201`,
		selector.CourseID, selector.Revision, selector.AnalysisVersion, selector.Model,
		selector.UnitKey, selector.UnitKey)
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

func (p sqlitePractice) ListQuestions(ctx context.Context, setIDs []int64) ([]database.PracticeQuestionRow, error) {
	out := []database.PracticeQuestionRow{}
	if len(setIDs) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(setIDs))
	for _, id := range setIDs {
		args = append(args, id)
	}
	// length(payload_json) undercounts multibyte payloads; casting to BLOB
	// measures UTF-8 bytes exactly like octet_length.
	rows, err := p.db.Query(ctx, `SELECT set_id,question_id,course_id,unit_key,question_key,
			CASE WHEN length(CAST(payload_json AS BLOB))<=65536 THEN payload_json END
		FROM practice_questions
		WHERE set_id IN(`+practiceSQLitePlaceholders(len(args))+`)
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

func (p sqlitePractice) QuestionProgress(
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
	// SQLite supports window functions and FILTER directly; CASE aggregates
	// below avoid FILTER (SQLite supports it since 3.30, modernc is newer,
	// but CASE keeps the aggregate semantics explicit on both drivers).
	rows, err := p.db.Query(ctx, `WITH history AS (
			SELECT question_id,outcome,attempted_at,
				ROW_NUMBER() OVER(
					PARTITION BY question_id ORDER BY attempted_at DESC,id DESC) rn
			FROM practice_attempts
			WHERE course_id=? AND question_id IN(`+practiceSQLitePlaceholders(len(questions))+`)
		) SELECT question_id,COUNT(*),
			COALESCE(MIN(CASE WHEN outcome<>'correct' THEN rn END),COUNT(*)+1)-1,
			MAX(CASE WHEN rn=1 THEN outcome END),MAX(CASE WHEN rn=1 THEN attempted_at END)
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

func (p sqlitePractice) AdmissionQuestion(
	ctx context.Context, selector database.PracticeAdmissionSelector,
) (database.PracticeAdmission, error) {
	var row database.PracticeAdmission
	// JSON1 json_extract reads the TEXT overview payload natively; the
	// publisher writes usable as a JSON boolean.
	err := p.db.QueryRow(ctx, `SELECT q.unit_key,q.question_key,s.set_hash,s.blueprint_revision_hash,
			CASE WHEN length(CAST(q.payload_json AS BLOB))<=65536 THEN q.payload_json END,c.payload
		FROM navigation n
		JOIN roadmap_content c ON c.content_id=n.content_id
		JOIN practice_question_sets s
			ON s.course_id=n.course_id AND s.blueprint_revision_hash=n.revision_id
		JOIN practice_questions q
			ON q.set_id=s.id AND q.course_id=s.course_id AND q.unit_key=s.unit_key
		WHERE n.course_id=? AND n.source_generation=? AND n.config_generation=?
			AND json_extract(n.overview,'$.usable')=1
			AND s.status='ready' AND s.analysis_version=? AND s.requested_model=?
			AND q.question_id=?`,
		selector.CourseID, selector.Generation, selector.Config,
		selector.AnalysisVersion, selector.Model, selector.Question,
	).Scan(&row.Unit, &row.Key, &row.Set, &row.Revision, &row.Payload, &row.Evidence)
	return row, err
}

func (p sqlitePractice) AttemptByKey(ctx context.Context, key string) (database.PracticeAttemptRow, error) {
	var row database.PracticeAttemptRow
	err := p.db.QueryRow(ctx, `SELECT id,course_id,unit_key,question_id,outcome,idempotency_key,
			confidence,seconds,answer,note,study_event_id,question_key,set_hash,blueprint_revision_hash
		FROM practice_attempts WHERE idempotency_key=?`, key,
	).Scan(
		&row.ID, &row.CourseID, &row.Unit, &row.Question, &row.Outcome, &row.Key,
		&row.Confidence, &row.Seconds, &row.Answer, &row.Note, &row.EventID,
		&row.QuestionKey, &row.Set, &row.Revision,
	)
	return row, err
}

func (p sqlitePractice) InsertAttempt(
	ctx context.Context, params database.PracticeAttemptParams,
) (int64, int64, error) {
	var eventID, attemptID int64
	err := p.db.QueryRow(ctx, `INSERT INTO study_unit_events
			(course_id,plan_revision,action_id,unit_key,event_type,idempotency_key,confidence,score,note)
		VALUES(?,?,?,?, 'recall_answered',?,?,?) RETURNING id`,
		params.CourseID, params.Revision, params.Question, params.Unit,
		params.EventKey, params.Confidence, params.Score, params.Note,
	).Scan(&eventID)
	if err != nil {
		return 0, 0, err
	}
	err = p.db.QueryRow(ctx, `INSERT INTO practice_attempts
			(course_id,unit_key,question_id,question_key,set_hash,blueprint_revision_hash,
			outcome,grading_mode,confidence,seconds,answer,note,idempotency_key,study_event_id)
		VALUES(?,?,?,?,?,?,?,'self',?,?,?,?,?,?) RETURNING id`,
		params.CourseID, params.Unit, params.Question, params.Key, params.Set, params.Revision,
		params.Outcome, params.Confidence, params.Seconds, params.Answer, params.Note,
		params.IdemKey, eventID,
	).Scan(&attemptID)
	if err != nil {
		return 0, 0, err
	}
	return attemptID, eventID, nil
}

func (p sqlitePractice) SetStatusCounts(
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
			SELECT id,status FROM (
				SELECT id,status,
					ROW_NUMBER() OVER(
						PARTITION BY unit_key ORDER BY (status='ready') DESC,id DESC) rn
				FROM practice_question_sets
				WHERE course_id=? AND blueprint_revision_hash=?
					AND analysis_version=? AND requested_model=?
					AND unit_key IN(`+practiceSQLitePlaceholders(len(units))+`)
					AND status IN('ready','pending','running','failed')
			) WHERE rn=1
		) SELECT status,COUNT(*),
			COALESCE(SUM((SELECT COUNT(*) FROM practice_questions q WHERE q.set_id=s.id)),0)
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
