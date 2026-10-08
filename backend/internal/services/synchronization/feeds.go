package synchronization

import (
	"context"
	"errors"
	"fmt"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
)

func (s Service) SaveAnnouncements(ctx context.Context, courseID int64, items []eclass.Announcement) error {
	return s.saveExtras(ctx, courseID, func(tx rdbms.Tx) error {
		lines := []string{}
		ids := []string{}
		for _, a := range items {
			if a.ID == "" {
				return errors.New("announcement identity is required")
			}
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.announcements WHERE course_id=$1 AND announcement_id=$2)`, courseID, identity.Encode(a.ID)).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				lines = append(lines, announcementLine(a))
				ids = append(ids, a.ID)
			}
			_, err := tx.Exec(
				ctx,
				`INSERT INTO app.announcements(course_id,announcement_id,title,link,description,pub_date) VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(course_id,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,description=excluded.description,pub_date=excluded.pub_date,fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
				courseID,
				identity.Encode(a.ID),
				identity.Encode(a.Title),
				a.Link,
				identity.Encode(a.Description),
				a.Published,
			)
			if err != nil {
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
	return s.saveExtras(ctx, courseID, func(tx rdbms.Tx) error {
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
			_, err = tx.Exec(
				ctx,
				`INSERT INTO app.exercises(course_id,exercise_id,title,link,deadline,submission_status,grade,work_type,description,start_date,max_grade,assignment_file_name,assignment_file_url,grade_comments,submission_date)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT(course_id,exercise_id) DO UPDATE SET title=excluded.title,link=excluded.link,deadline=excluded.deadline,submission_status=excluded.submission_status,grade=excluded.grade,work_type=excluded.work_type,description=excluded.description,start_date=excluded.start_date,max_grade=excluded.max_grade,assignment_file_name=excluded.assignment_file_name,assignment_file_url=excluded.assignment_file_url,grade_comments=excluded.grade_comments,submission_date=excluded.submission_date,fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
				courseID,
				identity.Encode(ex.ID),
				identity.Encode(ex.Title),
				ex.Link,
				identity.Encode(ex.Deadline),
				ex.SubmissionStatus,
				identity.Encode(ex.Grade),
				identity.Encode(ex.WorkType),
				identity.Encode(ex.Description),
				identity.Encode(ex.StartDate),
				identity.Encode(ex.MaxGrade),
				identity.Encode(ex.AssignmentFileName),
				ex.AssignmentFileURL,
				identity.Encode(ex.GradeComments),
				identity.Encode(ex.SubmissionDate),
			)
			if err != nil {
				return err
			}
		}
		return announceExercises(ctx, tx, courseID, lines)
	})
}

func (s Service) saveExtras(ctx context.Context, courseID int64, save func(rdbms.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var hidden int64
	if err = tx.QueryRow(ctx, `SELECT hidden FROM app.courses WHERE id=$1 FOR UPDATE`, courseID).Scan(&hidden); err != nil {
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
