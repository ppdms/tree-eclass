package rdbms

import (
	"context"
	"fmt"

	"tree-eclass/internal/domain/database"
)

type postgresSync struct{ db nativeDBTX }

func (s postgresSync) TryLockCourse(ctx context.Context, courseID int64) (bool, error) {
	return advisoryLock(ctx, s.db, fmt.Sprintf("eclass-sync:%d", courseID), true, true)
}

func (s postgresSync) LockGlobalFeed(ctx context.Context, feed string) error {
	_, err := advisoryLock(ctx, s.db, "global-feed:"+feed, true, false)
	return err
}

func (s postgresSync) LockCourseForSync(ctx context.Context, courseID int64) (database.SyncCourseGuard, error) {
	var guard database.SyncCourseGuard
	err := s.db.QueryRow(ctx, `SELECT name,webdav_folder,hidden FROM app.courses WHERE id=$1 FOR UPDATE`,
		courseID).Scan(&guard.Name, &guard.WebdavFolder, &guard.Hidden)
	return guard, err
}

func (s postgresSync) CourseHidden(ctx context.Context, courseID int64) (int64, error) {
	var hidden int64
	err := s.db.QueryRow(ctx, `SELECT hidden FROM app.courses WHERE id=$1 FOR UPDATE`, courseID).Scan(&hidden)
	return hidden, err
}

func (s postgresSync) CourseName(ctx context.Context, courseID int64) (string, error) {
	var name string
	err := s.db.QueryRow(ctx, `SELECT name FROM app.courses WHERE id=$1`, courseID).Scan(&name)
	return name, err
}

func (s postgresSync) CourseVisible(ctx context.Context, courseID int64) (bool, error) {
	var visible bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.courses WHERE id=$1 AND hidden=0)`,
		courseID).Scan(&visible)
	return visible, err
}

func (s postgresSync) ListTreeDirectories(ctx context.Context, courseID int64) ([]database.SyncDirectoryRow, error) {
	rows, err := s.db.Query(ctx, `SELECT n.id,n.course_id,n.parent_id,n.name,n.url,n.local_path
		FROM app.nodes n WHERE n.course_id=$1 ORDER BY n.id`, courseID)
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

func (s postgresSync) ListTreeFiles(ctx context.Context, courseID int64) ([]database.SyncFileRow, error) {
	rows, err := s.db.Query(ctx, `SELECT n.local_path,f.node_id,f.url,f.name,
			coalesce(f.local_path,''),coalesce(f.md5_hash,''),coalesce(f.etag,''),
			coalesce(f.redirect_url,''),coalesce(f.last_updated,''),coalesce(f.revision_id,''),
			o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM app.files f JOIN app.nodes n ON n.id=f.node_id
		LEFT JOIN app.objects o ON o.id=f.object_id WHERE n.course_id=$1 ORDER BY f.id`, courseID)
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

func (s postgresSync) DeleteTree(ctx context.Context, courseID int64) error {
	_, err := s.db.Exec(ctx, `DELETE FROM app.nodes WHERE course_id=$1`, courseID)
	return err
}

func (s postgresSync) InsertDirectory(ctx context.Context, dir database.SyncDirectoryInput) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx, `INSERT INTO app.nodes(course_id,parent_id,name,url,local_path)
		VALUES($1,$2,$3,$4,$5) RETURNING id`,
		dir.CourseID, dir.ParentID, dir.Name, dir.URL, dir.Path).Scan(&id)
	return id, err
}

func (s postgresSync) InsertFile(ctx context.Context, file database.SyncFileInput) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.files(node_id,url,name,md5_hash,etag,redirect_url,
			last_updated,local_path,object_id,revision_id)
		VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10)`,
		file.NodeID, file.URL, file.Name, file.MD5, file.ETag, file.Redirect,
		file.Updated, file.Path, file.ObjectID, file.RevisionID)
	return err
}

func (s postgresSync) RetireMissingEclassDocuments(ctx context.Context, courseID int64, current []string) error {
	_, err := s.db.Exec(ctx, `UPDATE knowledge.documents d SET is_current=0
		WHERE d.course_id=$1 AND d.source_origin='eclass' AND NOT(d.id=ANY($2::text[]))
		AND NOT EXISTS(SELECT 1 FROM knowledge.archive_members m WHERE m.child_document_id=d.id)`,
		courseID, current)
	return err
}
