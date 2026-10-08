package courses

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

// RefreshCoverage processes at most one course per tick. PostgreSQL snapshots
// keep all aggregate inputs consistent; publishing their generation rather than
// the current wall clock prevents a concurrent source edit being called fresh.
func (s Service) RefreshCoverage(ctx context.Context) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id, generation, learner int64
	err = tx.QueryRow(ctx, `SELECT c.id,g.generation,coalesce(l.generation,0)
 FROM app.courses c JOIN read_model.course_generation g ON g.course_id=c.id
 LEFT JOIN read_model.learner_generation l ON l.course_id=c.id LEFT JOIN read_model.course_coverage p ON p.course_id=c.id
 WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans e WHERE e.course_id=c.id AND e.enabled=1))
 AND (p.course_id IS NULL OR p.generation<>g.generation OR p.learner_generation<>coalesce(l.generation,0)) ORDER BY c.id LIMIT 1`).Scan(&id, &generation, &learner)
	if errors.Is(err, rdbms.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	payload, err := buildCoverage(ctx, tx, id)
	if err != nil {
		return false, err
	}
	recent, err := buildRecent(ctx, tx, id)
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	recentData, err := json.Marshal(recent)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO read_model.course_coverage(course_id,payload_json,generated_at,generation,learner_generation,recent_json)
 VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(course_id) DO UPDATE SET payload_json=excluded.payload_json,generated_at=excluded.generated_at,generation=excluded.generation,learner_generation=excluded.learner_generation,recent_json=excluded.recent_json`,
		id,
		string(data),
		time.Now().UTC().Format(time.RFC3339Nano),
		generation,
		learner,
		recentData,
	)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func buildCoverage(ctx context.Context, tx rdbms.Tx, id int64) (coveragePayload, error) {
	result := coveragePayload{Distribution: emptyDistribution(), Levels: map[string]int64{}}
	rows, err := tx.Query(ctx, `SELECT file_path,level FROM app.file_study WHERE course_id=$1`, id)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var path string
		var level int64
		if err = rows.Scan(&path, &level); err != nil {
			rows.Close()
			return result, err
		}
		result.Levels[identity.Decode(path)] = level
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	// Study distribution and completion describe currently registered files.
	// Historical levels stay in the path map but cannot inflate completion.
	rows, err = tx.Query(ctx, `WITH paths AS (
 SELECT f.local_path path FROM app.files f JOIN app.nodes n ON n.id=f.node_id WHERE n.course_id=$1
 UNION SELECT source_path FROM knowledge.documents WHERE course_id=$1 AND is_current=1 AND source_origin='external'
 ) SELECT coalesce(s.level,0),count(*) FROM paths p LEFT JOIN app.file_study s ON s.course_id=$1 AND s.file_path=p.path GROUP BY coalesce(s.level,0)`, id)
	if err != nil {
		return result, err
	}
	var score int64
	for rows.Next() {
		var level, count int64
		if err = rows.Scan(&level, &count); err != nil {
			rows.Close()
			return result, err
		}
		level = min(5, max(0, level))
		result.Distribution[strconv.FormatInt(level, 10)] = count
		if level < 5 {
			result.Total += count
			score += level * count
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	if result.Total > 0 {
		result.Completion = float64(score) / float64(4*result.Total)
	}
	err = tx.QueryRow(ctx, `SELECT count(*) FROM knowledge.documents WHERE course_id=$1 AND is_current=1 AND status='ready'`, id).
		Scan(&result.Indexed)
	return result, err
}

func buildRecent(ctx context.Context, tx rdbms.Tx, id int64) ([]RecentMaterial, error) {
	result := []RecentMaterial{}
	rows, err := tx.Query(
		ctx,
		`SELECT d.id,d.course_id,c.name,d.source_path,d.display_name,d.indexed_at FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id WHERE d.course_id=$1 AND d.is_current=1 AND d.status='ready' ORDER BY coalesce(d.indexed_at,'') DESC,d.id DESC LIMIT 6`,
		id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item RecentMaterial
		if err = rows.Scan(&item.ID, &item.CourseID, &item.CourseName, &item.Path, &item.Name, &item.Indexed); err != nil {
			return nil, err
		}
		item.CourseName, item.Path, item.Name = identity.Decode(
			item.CourseName,
		), identity.Decode(
			item.Path,
		), identity.Decode(
			item.Name,
		)
		result = append(result, item)
	}
	return result, rows.Err()
}
