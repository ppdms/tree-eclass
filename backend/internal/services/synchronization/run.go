package synchronization

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
)

func (s Service) Run(ctx context.Context, request Request, base string) (Result, error) {
	source, prefs, err := s.connect(ctx, base)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	courses, err := s.Pool.Courses().ListCourses(ctx, true)
	if err != nil {
		return Result{}, err
	}
	combined := Result{Changes: []Change{}}
	failures, err := s.syncCourses(ctx, source, request, base, courses, &combined)
	if err != nil {
		return combined, err
	}
	if request.CourseID == nil && len(courses) > 0 {
		if err = s.globalFeeds(ctx, source, prefs); err != nil {
			failures = append(failures, err)
		}
	}
	return combined, errors.Join(failures...)
}
func (s *Service) connect(ctx context.Context, base string) (*eclass.Client, settings.Preferences, error) {
	config := settings.Service{Pool: s.Pool}
	credentials, err := config.Credentials(ctx)
	if err != nil {
		return nil, settings.Preferences{}, err
	}
	if credentials == nil {
		return nil, settings.Preferences{}, eclass.ErrAuthentication
	}
	source, err := eclass.New(base, credentials.Username, credentials.Password)
	if err != nil {
		return nil, settings.Preferences{}, err
	}
	prefs, err := config.Preferences(ctx)
	if err != nil {
		return nil, settings.Preferences{}, err
	}
	s.MirrorRoot, err = resolveMirrorRoot(prefs.BasePath)
	if err != nil {
		return nil, settings.Preferences{}, err
	}
	source.SetTimeout(time.Duration(prefs.Timeout) * time.Second)
	if err = source.Login(ctx); err != nil {
		return nil, settings.Preferences{}, err
	}
	return source, prefs, nil
}
func (s Service) syncCourses(
	ctx context.Context,
	source *eclass.Client,
	request Request,
	base string,
	courses []database.AppCourse,
	combined *Result,
) ([]error, error) {
	failures := []error{}
	for _, course := range courses {
		if request.CourseID != nil && *request.CourseID != course.ID {
			continue
		}
		observed, err := s.markCourse(ctx, source, course.ID, base)
		combined.Added += observed.Added
		combined.Modified += observed.Modified
		combined.Deleted += observed.Deleted
		combined.FilesAdded += observed.FilesAdded
		combined.FilesChanged += observed.FilesChanged
		if err != nil {
			failures = append(failures, fmt.Errorf("course %d: %w", course.ID, err))
		}
		if ctx.Err() != nil {
			return failures, ctx.Err()
		}
	}
	return failures, nil
}
func (s Service) markCourse(ctx context.Context, source *eclass.Client, id int64, base string) (Result, error) {
	if err := s.Pool.Sync().MarkCourseChecking(ctx, id); err != nil {
		return Result{}, err
	}
	return s.checkCourse(ctx, source, id, base)
}

func (s Service) checkCourse(ctx context.Context, source *eclass.Client, id int64, base string) (Result, error) {
	root := fmt.Sprintf("%s/modules/document/index.php?course=INF%d", base, id)
	result, documentsErr := s.Sync(ctx, id, source, root)
	announcements, announcementsErr := source.Announcements(ctx, id)
	if announcementsErr == nil {
		announcementsErr = s.SaveAnnouncements(ctx, id, announcements)
	}
	exercises, exercisesErr := source.Exercises(ctx, id)
	if exercisesErr == nil {
		exercisesErr = s.SaveExercises(ctx, id, exercises)
	}
	return result, errors.Join(documentsErr, announcementsErr, exercisesErr)
}

func resolveMirrorRoot(configured string) (string, error) {
	if configured == "" {
		return "", nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	relative := strings.TrimPrefix(filepath.FromSlash(configured), string(filepath.Separator))
	if relative == "" || !filepath.IsLocal(relative) {
		return "", errors.New("download base path must be a non-root local path")
	}
	for _, part := range strings.Split(filepath.ToSlash(relative), "/") {
		if part == "." || part == ".." {
			return "", errors.New("download base path must not contain dot segments")
		}
	}
	return filepath.Join(home, relative), nil
}

func (s Service) Finish(ctx context.Context, result Result, failure error) error {
	status := "success"
	var message *string
	if failure != nil {
		status = "error"
		text := []rune(failure.Error())
		value := string(text[:min(len(text), 1000)])
		message = &value
	}
	now := time.Now().UTC().Format(time.RFC3339)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Sync().FinishCheck(ctx, database.SyncCheckFinish{
		At:          now,
		Status:      status,
		Error:       message,
		FilesAdded:  result.FilesAdded,
		FilesChange: result.FilesChanged,
	}); err != nil {
		return err
	}
	if failure != nil {
		err = notifications.EnqueueTx(
			ctx,
			tx,
			notifications.Event{
				Key:    "check-error:" + time.Now().UTC().Format(time.RFC3339Nano),
				Header: "**Course check failed**",
				Lines:  []string{"The check could not complete. Open tree-eclass for details."},
				Error:  true,
			},
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
