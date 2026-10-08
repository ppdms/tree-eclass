package synchronization

import (
	"context"
	"errors"
	"fmt"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
)

func (s Service) SaveAnnouncements(ctx context.Context, courseID int64, items []eclass.Announcement) error {
	return s.saveExtras(ctx, courseID, func(tx database.Tx) error {
		lines := []string{}
		ids := []string{}
		for _, a := range items {
			if a.ID == "" {
				return errors.New("announcement identity is required")
			}
			exists, err := tx.Sync().AnnouncementExists(ctx, courseID, identity.Encode(a.ID))
			if err != nil {
				return err
			}
			if !exists {
				lines = append(lines, announcementLine(a))
				ids = append(ids, a.ID)
			}
			if err := tx.Sync().UpsertAnnouncement(ctx, database.SyncAnnouncementInput{
				CourseID:    courseID,
				ID:          identity.Encode(a.ID),
				Title:       identity.Encode(a.Title),
				Link:        a.Link,
				Description: identity.Encode(a.Description),
				Published:   a.Published,
			}); err != nil {
				return err
			}
		}
		if len(lines) == 0 {
			return nil
		}
		header, err := courseHeader(ctx, tx, courseID, "New announcements")
		if err != nil {
			return err
		}
		return notifications.EnqueueTx(
			ctx,
			tx,
			notifications.Event{
				Key:    identity.Stable("announcements", append([]string{fmt.Sprint(courseID)}, ids...)...),
				Header: header,
				Lines:  lines,
			},
		)
	})
}

func (s Service) SaveExercises(ctx context.Context, courseID int64, items []eclass.Exercise) error {
	return s.saveExtras(ctx, courseID, func(tx database.Tx) error {
		lines := []string{}
		for _, ex := range items {
			if ex.ID == "" {
				return errors.New("exercise identity is required")
			}
			events, err := exerciseEvents(ctx, tx, courseID, ex)
			if err != nil {
				return err
			}
			lines = append(lines, events...)
			if err := tx.Sync().UpsertExercise(ctx, database.SyncExerciseInput{
				CourseID:           courseID,
				ID:                 identity.Encode(ex.ID),
				Title:              identity.Encode(ex.Title),
				Link:               ex.Link,
				Deadline:           identity.Encode(ex.Deadline),
				SubmissionStatus:   ex.SubmissionStatus,
				Grade:              identity.Encode(ex.Grade),
				WorkType:           identity.Encode(ex.WorkType),
				Description:        identity.Encode(ex.Description),
				StartDate:          identity.Encode(ex.StartDate),
				MaxGrade:           identity.Encode(ex.MaxGrade),
				AssignmentFileName: identity.Encode(ex.AssignmentFileName),
				AssignmentFileURL:  ex.AssignmentFileURL,
				GradeComments:      identity.Encode(ex.GradeComments),
				SubmissionDate:     identity.Encode(ex.SubmissionDate),
			}); err != nil {
				return err
			}
		}
		return announceExercises(ctx, tx, courseID, lines)
	})
}

func (s Service) saveExtras(ctx context.Context, courseID int64, save func(database.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Sync().CourseHidden(ctx, courseID); err != nil {
		return err
	}
	if err = save(tx); err != nil {
		return err
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
