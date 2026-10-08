package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqlitePDFDifferences struct{ db nativeDBTX }

func scanSQLitePDFDifference(row nativeRow) (database.PDFDifference, error) {
	var diff database.PDFDifference
	err := row.Scan(&diff.ID, &diff.CourseID, &diff.OldObjectID, &diff.NewObjectID,
		&diff.ToolVersion, &diff.Status, &diff.ObjectID, &diff.Error)
	return diff, err
}

func (d sqlitePDFDifferences) EnqueueDifference(
	ctx context.Context, params database.PDFDifferenceParams,
) (bool, string, error) {
	result, err := d.db.Exec(ctx, `INSERT INTO pdf_differences(id,course_id,old_object_id,new_object_id,tool_version)
		VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING`,
		params.ID, params.CourseID, params.OldObjectID, params.NewObjectID, params.ToolVersion)
	if err != nil {
		return false, "", err
	}
	status, err := d.DifferenceStatus(ctx, params.ID)
	if err != nil {
		return false, "", err
	}
	return result.RowsAffected() > 0, status, nil
}

func (d sqlitePDFDifferences) Difference(ctx context.Context, id string) (database.PDFDifference, error) {
	return scanSQLitePDFDifference(d.db.QueryRow(ctx, `SELECT id,course_id,old_object_id,new_object_id,
		tool_version,status,object_id,error FROM pdf_differences WHERE id=?`, id))
}

func (d sqlitePDFDifferences) LockDifference(ctx context.Context, id string) (database.PDFDifference, error) {
	// The admitted writer owns the row until commit/rollback; no row-lock
	// clause exists on this backend.
	return scanSQLitePDFDifference(d.db.QueryRow(ctx, `SELECT id,course_id,old_object_id,new_object_id,
		tool_version,status,object_id,error FROM pdf_differences WHERE id=?`, id))
}

func (d sqlitePDFDifferences) MarkDifferenceRunning(ctx context.Context, id string) error {
	_, err := d.db.Exec(ctx, `UPDATE pdf_differences SET status='running',error=NULL WHERE id=?`, id)
	return err
}

func (d sqlitePDFDifferences) MarkDifferenceFailed(ctx context.Context, id, message string) error {
	_, err := d.db.Exec(ctx, `UPDATE pdf_differences SET status='failed',error=?
		WHERE id=? AND status='running'`, message, id)
	return err
}

func (d sqlitePDFDifferences) PublishDifference(
	ctx context.Context, publication database.PDFDifferencePublication,
) error {
	status := "identical"
	var alias *string
	if publication.ObjectID != nil {
		value := "/_diffs/" + publication.ID + ".pdf"
		alias = &value
		status = "ready"
	}
	if _, err := d.db.Exec(ctx, `UPDATE pdf_differences SET status=?,object_id=?,error=NULL WHERE id=?`,
		status, publication.ObjectID, publication.ID); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `UPDATE change_record_items SET diff_webdav_path=? WHERE pdf_difference_id=?`,
		alias, publication.ID); err != nil {
		return err
	}
	_, err := d.db.Exec(ctx, `UPDATE file_versions SET diff_webdav_path=? WHERE pdf_difference_id=?`,
		alias, publication.ID)
	return err
}

func (d sqlitePDFDifferences) ReadyDifferenceObject(
	ctx context.Context, id string,
) (database.ObjectReference, error) {
	var object database.ObjectReference
	err := d.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM pdf_differences d
		JOIN objects o ON o.id=d.object_id
		JOIN courses c ON c.id=d.course_id AND c.hidden=0
		WHERE d.id=? AND d.status='ready'`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID, &object.SHA256, &object.Bytes, &object.MediaType)
	return object, err
}

func (d sqlitePDFDifferences) DifferenceStatus(ctx context.Context, id string) (string, error) {
	var status string
	err := d.db.QueryRow(ctx, `SELECT status FROM pdf_differences WHERE id=?`, id).Scan(&status)
	return status, err
}
