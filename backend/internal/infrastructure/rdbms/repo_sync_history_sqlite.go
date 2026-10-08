package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func (s sqliteSync) InsertChangeRecord(
	ctx context.Context, courseID int64, number, message string, count int,
) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO change_records(course_id,change_no,message,changes_count)
		VALUES(?,?,?,?) RETURNING id`, courseID, number, message, count).Scan(&id)
	return id, err
}

func (s sqliteSync) InsertChangeHistory(ctx context.Context, courseID int64, changeType, path string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO change_history(course_id,change_type,file_path)
		VALUES(?,?,?)`, courseID, changeType, path)
	return err
}

func (s sqliteSync) InsertChangeRecordItem(ctx context.Context, item database.SyncChangeRecordItemInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO change_record_items(change_record_id,change_type,file_path,
			display_name,redirect_url,pdf_difference_id,diff_webdav_path)
		VALUES(?,?,?,?,NULLIF(?,'') ,?,?)`,
		item.RecordID, item.Type, item.Path, item.Name, item.Redirect, item.Difference, item.DiffAlias)
	return err
}

func (s sqliteSync) InsertFileVersion(ctx context.Context, version database.SyncFileVersionInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO file_versions(course_id,file_path,version_webdav_path,change_type,
			display_name,redirect_url,revision_id,pdf_difference_id,diff_webdav_path)
		VALUES(?,?,?,?,?,NULLIF(?,'') ,?,?,?)`,
		version.CourseID, version.Path, version.StoragePath, version.Type, version.Name,
		version.Redirect, version.Revision, version.Difference, version.DiffAlias)
	return err
}

func (s sqliteSync) AnnouncementExists(ctx context.Context, courseID int64, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM announcements
		WHERE course_id=? AND announcement_id=?)`, courseID, id).Scan(&exists)
	return exists, err
}

func (s sqliteSync) UpsertAnnouncement(ctx context.Context, announcement database.SyncAnnouncementInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO announcements(course_id,announcement_id,title,link,description,pub_date)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(course_id,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		description=excluded.description,pub_date=excluded.pub_date,
		fetched_at=strftime('%Y-%m-%d %H:%M:%S','now')`,
		announcement.CourseID, announcement.ID, announcement.Title, announcement.Link,
		announcement.Description, announcement.Published)
	return err
}

func (s sqliteSync) ExerciseState(ctx context.Context, courseID int64, id string) (database.SyncExerciseState, error) {
	var state database.SyncExerciseState
	err := s.db.QueryRow(ctx, `SELECT coalesce(grade,''),coalesce(grade_comments,''),
			coalesce(assignment_file_name,''),coalesce(assignment_file_url,'')
		FROM exercises WHERE course_id=? AND exercise_id=?`, courseID, id).
		Scan(&state.Grade, &state.GradeComments, &state.AssignmentFileName, &state.AssignmentFileURL)
	return state, err
}

func (s sqliteSync) UpsertExercise(ctx context.Context, exercise database.SyncExerciseInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO exercises(course_id,exercise_id,title,link,deadline,submission_status,
			grade,work_type,description,start_date,max_grade,assignment_file_name,assignment_file_url,
			grade_comments,submission_date)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(course_id,exercise_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		deadline=excluded.deadline,submission_status=excluded.submission_status,grade=excluded.grade,
		work_type=excluded.work_type,description=excluded.description,start_date=excluded.start_date,
		max_grade=excluded.max_grade,assignment_file_name=excluded.assignment_file_name,
		assignment_file_url=excluded.assignment_file_url,grade_comments=excluded.grade_comments,
		submission_date=excluded.submission_date,
		fetched_at=strftime('%Y-%m-%d %H:%M:%S','now')`,
		exercise.CourseID, exercise.ID, exercise.Title, exercise.Link, exercise.Deadline,
		exercise.SubmissionStatus, exercise.Grade, exercise.WorkType, exercise.Description,
		exercise.StartDate, exercise.MaxGrade, exercise.AssignmentFileName, exercise.AssignmentFileURL,
		exercise.GradeComments, exercise.SubmissionDate)
	return err
}

func (s sqliteSync) GlobalAnnouncementExists(ctx context.Context, feed, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM global_announcements
		WHERE feed_key=? AND announcement_id=?)`, feed, id).Scan(&exists)
	return exists, err
}

func (s sqliteSync) UpsertGlobalAnnouncement(
	ctx context.Context, announcement database.SyncGlobalAnnouncementInput,
) error {
	_, err := s.db.Exec(ctx, `INSERT INTO global_announcements(feed_key,announcement_id,title,link,description,pub_date)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(feed_key,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,
		description=excluded.description,pub_date=excluded.pub_date,
		fetched_at=strftime('%Y-%m-%d %H:%M:%S','now')`,
		announcement.FeedKey, announcement.ID, announcement.Title, announcement.Link,
		announcement.Description, announcement.Published)
	return err
}

func (s sqliteSync) ListVersions(
	ctx context.Context, courseID int64, kind string, file, folder *string,
) ([]database.SyncVersion, error) {
	// SQLite INSTR/SUBSTR replace starts_with/rtrim: a folder filter matches
	// the exact path or any path under folder with a slash boundary.
	rows, err := s.db.Query(ctx, `SELECT id,course_id,file_path,version_webdav_path,change_type,timestamp,
			display_name,redirect_url,diff_webdav_path FROM file_versions
		WHERE course_id=? AND change_type=?
		AND (? IS NULL OR file_path=?)
		AND (? IS NULL OR ?= '' OR file_path=? OR instr(file_path,rtrim(?,'/')||'/')=1)
		ORDER BY timestamp DESC,id DESC`,
		courseID, kind, file, file, folder, folder, folder, folder)
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

func (s sqliteSync) ChangeRecord(
	ctx context.Context, courseID int64, number string,
) (database.SyncChangeRecord, string, error) {
	var record database.SyncChangeRecord
	var course string
	err := s.db.QueryRow(ctx, `SELECT r.id,r.course_id,c.name,r.change_no,r.timestamp,r.message,r.changes_count
		FROM change_records r JOIN courses c ON c.id=r.course_id
		WHERE r.course_id=? AND r.change_no=? AND c.hidden=0`, courseID, number).
		Scan(&record.ID, &record.CourseID, &course, &record.Number,
			&record.Timestamp, &record.Message, &record.Count)
	return record, course, err
}

func (s sqliteSync) ChangeRecordItems(ctx context.Context, recordID int64) ([]database.SyncHistoryItem, error) {
	rows, err := s.db.Query(ctx, `SELECT change_type,file_path,display_name,redirect_url,diff_webdav_path
		FROM change_record_items WHERE change_record_id=? ORDER BY id`, recordID)
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

func (s sqliteSync) FindSyncClaim(ctx context.Context) (database.SyncClaim, error) {
	// The admitted writer serializes claims; no row-lock clause.
	// Payload binds as TEXT in storage; scan into bytes preserves it.
	var claim database.SyncClaim
	err := s.db.QueryRow(ctx, `SELECT id,status,payload,error FROM control_commands
		WHERE queue='sync' AND status IN ('pending','running') LIMIT 1`).
		Scan(&claim.ID, &claim.Status, &claim.Payload, &claim.Failure)
	return claim, err
}

func (s sqliteSync) ResetSyncClaim(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `UPDATE control_commands
		SET attempts=0,available_at=strftime('%Y-%m-%d %H:%M:%S','now'),claimed_at=NULL,error=NULL
		WHERE id=?`, id)
	return err
}

func (s sqliteSync) MarkCheckStart(ctx context.Context, start database.SyncCheckStart) error {
	_, err := s.db.Exec(ctx, `INSERT INTO check_status(id,is_checking,started_at,current_course_id)
		VALUES(1,1,?,?)
		ON CONFLICT(id) DO UPDATE SET is_checking=1,started_at=excluded.started_at,
		current_course_id=excluded.current_course_id`, start.At, start.CourseID)
	return err
}

func (s sqliteSync) MarkCourseChecking(ctx context.Context, courseID int64) error {
	_, err := s.db.Exec(ctx, `UPDATE check_status SET is_checking=1,current_course_id=? WHERE id=1`, courseID)
	return err
}

func (s sqliteSync) FinishCheck(ctx context.Context, finish database.SyncCheckFinish) error {
	_, err := s.db.Exec(ctx, `INSERT INTO check_status(id,is_checking,last_check_at,last_check_result,
			last_error,last_files_added,last_files_changed)
		VALUES(1,0,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET is_checking=0,current_course_id=NULL,
		last_check_at=excluded.last_check_at,last_check_result=excluded.last_check_result,
		last_error=excluded.last_error,last_files_added=excluded.last_files_added,
		last_files_changed=excluded.last_files_changed`,
		finish.At, finish.Status, finish.Error, finish.FilesAdded, finish.FilesChange)
	return err
}
