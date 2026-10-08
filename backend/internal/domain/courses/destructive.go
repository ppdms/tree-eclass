package courses

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/infrastructure/rdbms"
)

var ErrReplay = errors.New("this mutation request was already processed")
var ErrBusy = errors.New("this course is being synchronized or changed; retry after that operation finishes")
var mutationKey = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func ValidMutationKey(key string) bool { return mutationKey.MatchString(key) }

func (s Service) Destructive(ctx context.Context, id int64, action, key string) error {
	if (action != "delete" && action != "reset") || !ValidMutationKey(key) {
		return errors.New("invalid destructive intent")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("eclass-sync:%d", id)).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrBusy
	}
	tag, err := tx.Exec(
		ctx,
		`INSERT INTO app.course_mutations(action,course_id,idempotency_key) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		action,
		id,
		key,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrReplay
	}
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
		return err
	}
	if action == "delete" {
		err = deleteCourse(ctx, tx, id)
	} else {
		err = resetCourse(ctx, tx, id)
	}
	if err != nil {
		return err
	}
	if _, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func resetCourse(ctx context.Context, tx rdbms.Tx, id int64) error {
	for _, table := range []string{"nodes", "change_history", "change_records", "announcements", "file_versions"} {
		if _, err := tx.Exec(ctx, "DELETE FROM app."+table+" WHERE course_id=$1", id); err != nil {
			return err
		}
	}
	// User uploads and learner evidence survive an upstream reset. Historical
	// immutable revisions remain registered, but are no longer current files.
	_, err := tx.Exec(
		ctx,
		`UPDATE knowledge.documents SET is_current=0 WHERE course_id=$1 AND source_origin='eclass'`,
		id,
	)
	return err
}

func deleteCourse(ctx context.Context, tx rdbms.Tx, id int64) error {
	if _, err := tx.Exec(ctx, `DELETE FROM app.control_commands WHERE payload->>'document_id' IN (SELECT id FROM knowledge.documents WHERE course_id=$1)`, id); err != nil {
		return err
	}
	// These evidence tables predate cross-schema course FKs. Document children
	// and all learner tables have their own cascading constraints.
	for _, table := range []string{"practice_questions", "practice_question_sets", "course_blueprints", "index_jobs", "documents"} {
		if _, err := tx.Exec(ctx, "DELETE FROM knowledge."+table+" WHERE course_id=$1", id); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM app.control_commands WHERE payload->>'course_id'=$1`, fmt.Sprint(id)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM read_model.course_coverage WHERE course_id=$1`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM read_model.recent_materials WHERE course_id=$1`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM read_model.study_metrics WHERE scope=$1`, fmt.Sprintf("course:%d", id)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `DELETE FROM app.courses WHERE id=$1`, id)
	return err
}
