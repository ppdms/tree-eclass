package rdbms

import (
	"context"
	"fmt"
	"strings"

	"tree-eclass/internal/domain/database"
)

type sqliteSync struct{ db nativeDBTX }

func (s sqliteSync) TryLockCourse(ctx context.Context, courseID int64) (bool, error) {
	return advisoryLock(ctx, s.db, fmt.Sprintf("eclass-sync:%d", courseID), true, true)
}

func (s sqliteSync) LockGlobalFeed(ctx context.Context, feed string) error {
	_, err := advisoryLock(ctx, s.db, "global-feed:"+feed, true, false)
	return err
}

func (s sqliteSync) LockCourseForSync(ctx context.Context, courseID int64) (database.SyncCourseGuard, error) {
	var guard database.SyncCourseGuard
	err := s.db.QueryRow(ctx, `SELECT name,webdav_folder,hidden FROM courses WHERE id=?`,
		courseID).Scan(&guard.Name, &guard.WebdavFolder, &guard.Hidden)
	return guard, err
}

func (s sqliteSync) CourseHidden(ctx context.Context, courseID int64) (int64, error) {
	var hidden int64
	err := s.db.QueryRow(ctx, `SELECT hidden FROM courses WHERE id=?`, courseID).Scan(&hidden)
	return hidden, err
}

func (s sqliteSync) CourseName(ctx context.Context, courseID int64) (string, error) {
	var name string
	err := s.db.QueryRow(ctx, `SELECT name FROM courses WHERE id=?`, courseID).Scan(&name)
	return name, err
}

func (s sqliteSync) CourseVisible(ctx context.Context, courseID int64) (bool, error) {
	var visible bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM courses WHERE id=? AND hidden=0)`,
		courseID).Scan(&visible)
	return visible, err
}

func (s sqliteSync) ListTreeDirectories(ctx context.Context, courseID int64) ([]database.SyncDirectoryRow, error) {
	rows, err := s.db.Query(ctx, `SELECT n.id,n.course_id,n.parent_id,n.name,n.url,n.local_path
		FROM nodes n WHERE n.course_id=? ORDER BY n.id`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SyncDirectoryRow{}
	for rows.Next() {
		var dir database.SyncDirectoryRow
		if err := rows.Scan(&dir.ID, &dir.CourseID, &dir.ParentID, &dir.Name, &dir.URL, &dir.Path); err != nil {
			return nil, err
		}
		out = append(out, dir)
	}
	return out, rows.Err()
}

func (s sqliteSync) ListTreeFiles(ctx context.Context, courseID int64) ([]database.SyncFileRow, error) {
	rows, err := s.db.Query(ctx, `SELECT n.local_path,f.node_id,f.url,f.name,
			coalesce(f.local_path,''),coalesce(f.md5_hash,''),coalesce(f.etag,''),
			coalesce(f.redirect_url,''),coalesce(f.last_updated,''),coalesce(f.revision_id,''),
			o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM files f JOIN nodes n ON n.id=f.node_id
		LEFT JOIN objects o ON o.id=f.object_id WHERE n.course_id=? ORDER BY f.id`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SyncFileRow{}
	for rows.Next() {
		var file database.SyncFileRow
		var bucket, key, version, sha, media *string
		var size *int64
		if err := rows.Scan(&file.Parent, &file.NodeID, &file.URL, &file.Name, &file.Path, &file.MD5,
			&file.ETag, &file.Redirect, &file.Updated, &file.Revision,
			&bucket, &key, &version, &sha, &size, &media); err != nil {
			return nil, err
		}
		if bucket != nil {
			file.Object = &database.ObjectReference{
				Bucket: *bucket, Key: *key, VersionID: *version, SHA256: *sha, Bytes: *size, MediaType: *media,
			}
		}
		out = append(out, file)
	}
	return out, rows.Err()
}

func (s sqliteSync) DeleteTree(ctx context.Context, courseID int64) error {
	_, err := s.db.Exec(ctx, `DELETE FROM nodes WHERE course_id=?`, courseID)
	return err
}

func (s sqliteSync) InsertDirectory(ctx context.Context, dir database.SyncDirectoryInput) (int64, error) {
	// modernc exposes LastInsertId but the native result hides it; read back
	// the inserted row id on the same connection/transaction instead.
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO nodes(course_id,parent_id,name,url,local_path)
		VALUES(?,?,?,?,?) RETURNING id`,
		dir.CourseID, dir.ParentID, dir.Name, dir.URL, dir.Path).Scan(&id)
	return id, err
}

func (s sqliteSync) InsertFile(ctx context.Context, file database.SyncFileInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO files(node_id,url,name,md5_hash,etag,redirect_url,
			last_updated,local_path,object_id,revision_id)
		VALUES(?,?,?,?,?,NULLIF(?,'') ,?,?,?,?)`,
		file.NodeID, file.URL, file.Name, file.MD5, file.ETag, file.Redirect,
		file.Updated, file.Path, file.ObjectID, file.RevisionID)
	return err
}

func (s sqliteSync) RetireMissingEclassDocuments(ctx context.Context, courseID int64, current []string) error {
	// SQLite binds one parameter per id; empty current must retire every
	// non-member eClass document, matching NOT(id=ANY('{}')) on postgres.
	args := []any{courseID}
	filter := ""
	if len(current) > 0 {
		marks := make([]string, 0, len(current))
		for _, id := range current {
			marks = append(marks, "?")
			args = append(args, id)
		}
		filter = " AND id NOT IN (" + strings.Join(marks, ",") + ")"
	}
	_, err := s.db.Exec(ctx, `UPDATE documents SET is_current=0
		WHERE course_id=? AND source_origin='eclass'`+filter+`
		AND NOT EXISTS(SELECT 1 FROM archive_members m WHERE m.child_document_id=documents.id)`,
		args...)
	return err
}
