package courses

import (
	"context"
	"errors"
	"unicode/utf8"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// Study levels retain the exact logical catalog path, including deleted-file
// history. Folder preferences additionally require a currently registered node.
func (s Service) StudyLevel(ctx context.Context, id int64, path string, level int64) error {
	if path == "" || utf8.RuneCountInString(path) > 4096 || level < 0 || level > 5 {
		return errors.New("file_path and a level between 0 and 5 are required")
	}
	return s.studyMutation(ctx, id, func(tx database.Tx) error {
		err := tx.Courses().SetStudyLevel(ctx, database.SetStudyLevelParams{
			CourseID: id,
			FilePath: identity.Encode(path),
			Level:    level,
		})
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
	return s.studyMutation(ctx, id, func(tx database.Tx) error {
		if err := tx.Courses().NodeForFolder(ctx, id, identity.Encode(key)); err != nil {
			return err
		}
		flag := int64(0)
		if collapsed {
			flag = 1
		}
		return tx.Courses().SetFolderCollapsed(ctx, id, identity.Encode(key), flag)
	})
}

func (s Service) studyMutation(ctx context.Context, id int64, fn func(database.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Courses().LockCourseForWrite(ctx, id); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
