package courses

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// RefreshCoverage processes at most one course per tick. PostgreSQL snapshots
// keep all aggregate inputs consistent; publishing their generation rather than
// the current wall clock prevents a concurrent source edit being called fresh.
func (s Service) RefreshCoverage(ctx context.Context) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	target, err := tx.Courses().StaleCoverageTarget(ctx)
	if database.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	payload, err := buildCoverage(ctx, tx, target.CourseID)
	if err != nil {
		return false, err
	}
	recent, err := buildRecent(ctx, tx, target.CourseID)
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
	err = tx.Courses().PublishCoverage(ctx, database.PublishCoverageParams{
		CourseID:   target.CourseID,
		Payload:    string(data),
		Generated:  time.Now().UTC().Format(time.RFC3339Nano),
		Generation: target.Generation,
		Learner:    target.Learner,
		Recent:     recentData,
	})
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func buildCoverage(ctx context.Context, tx database.Tx, id int64) (coveragePayload, error) {
	result := coveragePayload{Distribution: emptyDistribution(), Levels: map[string]int64{}}
	levels, err := tx.Courses().StudyLevelsForCourse(ctx, id)
	if err != nil {
		return result, err
	}
	for _, level := range levels {
		result.Levels[identity.Decode(level.FilePath)] = level.Level
	}
	// Study distribution and completion describe currently registered files.
	// Historical levels stay in the path map but cannot inflate completion.
	buckets, err := tx.Courses().StudyDistribution(ctx, id)
	if err != nil {
		return result, err
	}
	var score int64
	for _, bucket := range buckets {
		level := min(5, max(0, bucket.Level))
		result.Distribution[strconv.FormatInt(level, 10)] = bucket.Count
		if level < 5 {
			result.Total += bucket.Count
			score += level * bucket.Count
		}
	}
	if result.Total > 0 {
		result.Completion = float64(score) / float64(4*result.Total)
	}
	result.Indexed, err = tx.Courses().CountReadyDocuments(ctx, id)
	return result, err
}

func buildRecent(ctx context.Context, tx database.Tx, id int64) ([]RecentMaterial, error) {
	rows, err := tx.Courses().RecentMaterialsForCourse(ctx, id)
	if err != nil {
		return nil, err
	}
	result := make([]RecentMaterial, 0, len(rows))
	for _, row := range rows {
		result = append(result, RecentMaterial{
			ID:         row.ID,
			CourseID:   row.CourseID,
			CourseName: identity.Decode(row.CourseName),
			Path:       identity.Decode(row.SourcePath),
			Name:       identity.Decode(row.Display),
			Indexed:    row.Indexed,
		})
	}
	return result, nil
}
