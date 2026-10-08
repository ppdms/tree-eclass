package courses

import (
	"context"
	"errors"
	"regexp"

	"tree-eclass/internal/domain/commands"
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
	locked, err := tx.Courses().TryLockCourseForMutation(ctx, id)
	if err != nil {
		return err
	}
	if !locked {
		return ErrBusy
	}
	claimed, err := tx.Courses().ClaimCourseMutation(ctx, action, id, key)
	if err != nil {
		return err
	}
	if !claimed {
		return ErrReplay
	}
	if err = tx.Courses().LockCourseForMutation(ctx, id); err != nil {
		return err
	}
	if action == "delete" {
		err = tx.Courses().DeleteCourse(ctx, id)
	} else {
		err = tx.Courses().ResetCourse(ctx, id)
	}
	if err != nil {
		return err
	}
	if _, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
