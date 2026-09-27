package courses

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/identity"
)

// Study levels retain the exact logical catalog path, including deleted-file
// history. Folder preferences additionally require a currently registered node.
func (s Service) StudyLevel(ctx context.Context, id int64, path string, level int64) error {
	if path == "" || utf8.RuneCountInString(path) > 4096 || level < 0 || level > 5 {
		return errors.New("file_path and a level between 0 and 5 are required")
	}
	return s.studyMutation(ctx, id, func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO app.file_study(course_id,file_path,level,last_updated) VALUES($1,$2,$3,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))
ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,last_updated=excluded.last_updated`,
			id,
			identity.Encode(path),
			level,
		)
		if err != nil {
			return err
		}
		_, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true)
		return err
	})
}

func (s Service) FolderCollapsed(ctx context.Context, id int64, key string, collapsed bool) error {
	if key == "" || utf8.RuneCountInString(key) > 4096 {
		return errors.New("a valid folder_key is required")
	}
	return s.studyMutation(ctx, id, func(tx pgx.Tx) error {
		var node int64
		if err := tx.QueryRow(ctx, `SELECT id FROM app.nodes WHERE course_id=$1 AND (url=$2 OR local_path=$2) LIMIT 1 FOR SHARE`, id, identity.Encode(key)).Scan(&node); err != nil {
			return err
		}
		flag := 0
		if collapsed {
			flag = 1
		}
		_, err := tx.Exec(
			ctx,
			`INSERT INTO app.collapsed_course_folders(course_id,folder_key,collapsed) VALUES($1,$2,$3)
ON CONFLICT(course_id,folder_key) DO UPDATE SET collapsed=$3,updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
			id,
			identity.Encode(key),
			flag,
		)
		return err
	})
}

func (s Service) studyMutation(ctx context.Context, id int64, fn func(pgx.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0 FOR SHARE`, id).Scan(&found); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
