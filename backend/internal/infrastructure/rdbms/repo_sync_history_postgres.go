package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func (s postgresSync) InsertChangeRecord(
	ctx context.Context, courseID int64, number, message string, count int,
) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO app.change_records(course_id,change_no,message,changes_count)
		VALUES($1,$2,$3,$4) RETURNING id`, courseID, number, message, count).Scan(&id)
	return id, err
}

func (s postgresSync) InsertChangeHistory(ctx context.Context, courseID int64, changeType, path string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.change_history(course_id,change_type,file_path)
		VALUES($1,$2,$3)`, courseID, changeType, path)
	return err
}

func (s postgresSync) InsertChangeRecordItem(ctx context.Context, item database.SyncChangeRecordItemInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.change_record_items(change_record_id,change_type,file_path,
			display_name,redirect_url,pdf_difference_id,diff_webdav_path)
		VALUES($1,$2,$3,$4,NULLIF($5,''),$6,$7)`,
		item.RecordID, item.Type, item.Path, item.Name, item.Redirect, item.Difference, item.DiffAlias)
	return err
}

func (s postgresSync) InsertFileVersion(ctx context.Context, version database.SyncFileVersionInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.file_versions(course_id,file_path,version_webdav_path,change_type,
			display_name,redirect_url,revision_id,pdf_difference_id,diff_webdav_path)
		VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9)`,
		version.CourseID, version.Path, version.StoragePath, version.Type, version.Name,
		version.Redirect, version.Revision, version.Difference, version.DiffAlias)
	return err
}

func (s postgresSync) AnnouncementExists(ctx context.Context, courseID int64, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.announcements
		WHERE course_id=$1 AND announcement_id=$2)`, courseID, id).Scan(&exists)
	return exists, err
}

func (s postgresSync) UpsertAnnouncement(ctx context.Context, announcement database.SyncAnnouncementInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.announcements(course_id,announcement_id,title,link,description,pub_date)
		VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT(course_id,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		description=excluded.description,pub_date=excluded.pub_date,
		fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		announcement.CourseID, announcement.ID, announcement.Title, announcement.Link,
		announcement.Description, announcement.Published)
	return err
}

func (s postgresSync) ExerciseState(
	ctx context.Context, courseID int64, id string,
) (database.SyncExerciseState, error) {
	var state database.SyncExerciseState
	err := s.db.QueryRow(ctx, `SELECT coalesce(grade,''),coalesce(grade_comments,''),
			coalesce(assignment_file_name,''),coalesce(assignment_file_url,'')
		FROM app.exercises WHERE course_id=$1 AND exercise_id=$2`, courseID, id).
		Scan(&state.Grade, &state.GradeComments, &state.AssignmentFileName, &state.AssignmentFileURL)
	return state, err
}

func (s postgresSync) UpsertExercise(ctx context.Context, exercise database.SyncExerciseInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.exercises(course_id,exercise_id,title,link,deadline,submission_status,
			grade,work_type,description,start_date,max_grade,assignment_file_name,assignment_file_url,
			grade_comments,submission_date)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT(course_id,exercise_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		deadline=excluded.deadline,submission_status=excluded.submission_status,grade=excluded.grade,
		work_type=excluded.work_type,description=excluded.description,start_date=excluded.start_date,
		max_grade=excluded.max_grade,assignment_file_name=excluded.assignment_file_name,
		assignment_file_url=excluded.assignment_file_url,grade_comments=excluded.grade_comments,
		submission_date=excluded.submission_date,
		fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		exercise.CourseID, exercise.ID, exercise.Title, exercise.Link, exercise.Deadline,
		exercise.SubmissionStatus, exercise.Grade, exercise.WorkType, exercise.Description,
		exercise.StartDate, exercise.MaxGrade, exercise.AssignmentFileName, exercise.AssignmentFileURL,
		exercise.GradeComments, exercise.SubmissionDate)
	return err
}

func (s postgresSync) GlobalAnnouncementExists(ctx context.Context, feed, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.global_announcements
		WHERE feed_key=$1 AND announcement_id=$2)`, feed, id).Scan(&exists)
	return exists, err
}

func (s postgresSync) UpsertGlobalAnnouncement(
	ctx context.Context, announcement database.SyncGlobalAnnouncementInput,
) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.global_announcements
		(feed_key,announcement_id,title,link,description,pub_date)
		VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT(feed_key,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		description=excluded.description,pub_date=excluded.pub_date,
		fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		announcement.FeedKey, announcement.ID, announcement.Title, announcement.Link,
		announcement.Description, announcement.Published)
	return err
}

func (s postgresSync) ListVersions(
	ctx context.Context, courseID int64, kind string, file, folder *string,
) ([]database.SyncVersion, error) {
	rows, err := s.db.Query(ctx, `SELECT id,course_id,file_path,version_webdav_path,change_type,timestamp,
			display_name,redirect_url,diff_webdav_path FROM app.file_versions
		WHERE course_id=$1 AND change_type=$2
		AND ($3::text IS NULL OR file_path=$3)
		AND ($4::text IS NULL OR $4='' OR file_path=$4 OR starts_with(file_path,rtrim($4,'/')||'/'))
		ORDER BY timestamp DESC,id DESC`, courseID, kind, file, folder)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SyncVersion{}
	for rows.Next() {
		var version database.SyncVersion
		if err := rows.Scan(&version.ID, &version.CourseID, &version.Path, &version.StoragePath,
			&version.Type, &version.Timestamp, &version.Name, &version.Redirect, &version.Diff); err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	return out, rows.Err()
}

func (s postgresSync) ChangeRecord(
	ctx context.Context, courseID int64, number string,
) (database.SyncChangeRecord, string, error) {
	var record database.SyncChangeRecord
	var course string
	err := s.db.QueryRow(ctx, `SELECT r.id,r.course_id,c.name,r.change_no,r.timestamp,r.message,r.changes_count
		FROM app.change_records r JOIN app.courses c ON c.id=r.course_id
		WHERE r.course_id=$1 AND r.change_no=$2 AND c.hidden=0`, courseID, number).
		Scan(&record.ID, &record.CourseID, &course, &record.Number,
			&record.Timestamp, &record.Message, &record.Count)
	return record, course, err
}

func (s postgresSync) ChangeRecordItems(ctx context.Context, recordID int64) ([]database.SyncHistoryItem, error) {
	rows, err := s.db.Query(ctx, `SELECT change_type,file_path,display_name,redirect_url,diff_webdav_path
		FROM app.change_record_items WHERE change_record_id=$1 ORDER BY id`, recordID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SyncHistoryItem{}
	for rows.Next() {
		var item database.SyncHistoryItem
		if err := rows.Scan(&item.Type, &item.Path, &item.Name, &item.Redirect, &item.Diff); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s postgresSync) FindSyncClaim(ctx context.Context) (database.SyncClaim, error) {
	var claim database.SyncClaim
	err := s.db.QueryRow(ctx, `SELECT id,status,payload,error FROM app.control_commands
		WHERE queue='sync' AND status IN ('pending','running') LIMIT 1 FOR UPDATE`).
		Scan(&claim.ID, &claim.Status, &claim.Payload, &claim.Failure)
	return claim, err
}

func (s postgresSync) ResetSyncClaim(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `UPDATE app.control_commands
		SET attempts=0,available_at=clock_timestamp(),claimed_at=NULL,error=NULL WHERE id=$1`, id)
	return err
}

func (s postgresSync) MarkCheckStart(ctx context.Context, start database.SyncCheckStart) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.check_status(id,is_checking,started_at,current_course_id)
		VALUES(1,1,$1,$2)
		ON CONFLICT(id) DO UPDATE SET is_checking=1,started_at=$1,current_course_id=$2`,
		start.At, start.CourseID)
	return err
}

func (s postgresSync) MarkCourseChecking(ctx context.Context, courseID int64) error {
	_, err := s.db.Exec(ctx, `UPDATE app.check_status SET is_checking=1,current_course_id=$1 WHERE id=1`, courseID)
	return err
}

func (s postgresSync) FinishCheck(ctx context.Context, finish database.SyncCheckFinish) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.check_status(id,is_checking,last_check_at,last_check_result,
			last_error,last_files_added,last_files_changed)
		VALUES(1,0,$1,$2,$3,$4,$5)
		ON CONFLICT(id) DO UPDATE SET is_checking=0,current_course_id=NULL,last_check_at=$1,
		last_check_result=$2,last_error=$3,last_files_added=$4,last_files_changed=$5`,
		finish.At, finish.Status, finish.Error, finish.FilesAdded, finish.FilesChange)
	return err
}
