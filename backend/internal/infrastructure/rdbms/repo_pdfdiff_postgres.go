package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresPDFDifferences struct{ db nativeDBTX }

func scanPDFDifference(row nativeRow) (database.PDFDifference, error) {
	var diff database.PDFDifference
	err := row.Scan(&diff.ID, &diff.CourseID, &diff.OldObjectID, &diff.NewObjectID,
		&diff.ToolVersion, &diff.Status, &diff.ObjectID, &diff.Error)
	return diff, err
}

func (d postgresPDFDifferences) EnqueueDifference(
	ctx context.Context, params database.PDFDifferenceParams,
) (bool, string, error) {
	result, err := d.db.Exec(ctx, `INSERT INTO app.pdf_differences(id,course_id,old_object_id,new_object_id,tool_version)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO NOTHING`,
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

func (d postgresPDFDifferences) Difference(ctx context.Context, id string) (database.PDFDifference, error) {
	return scanPDFDifference(d.db.QueryRow(ctx, `SELECT id,course_id,old_object_id,new_object_id,
		tool_version,status,object_id,error FROM app.pdf_differences WHERE id=$1`, id))
}

func (d postgresPDFDifferences) LockDifference(ctx context.Context, id string) (database.PDFDifference, error) {
	return scanPDFDifference(d.db.QueryRow(ctx, `SELECT id,course_id,old_object_id,new_object_id,
		tool_version,status,object_id,error FROM app.pdf_differences WHERE id=$1 FOR UPDATE`, id))
}

func (d postgresPDFDifferences) MarkDifferenceRunning(ctx context.Context, id string) error {
	_, err := d.db.Exec(ctx, `UPDATE app.pdf_differences SET status='running',error=NULL WHERE id=$1`, id)
	return err
}

func (d postgresPDFDifferences) MarkDifferenceFailed(ctx context.Context, id, message string) error {
	_, err := d.db.Exec(ctx, `UPDATE app.pdf_differences SET status='failed',error=$2`+
		` WHERE id=$1 AND status='running'`, id, message)
	return err
}

func (d postgresPDFDifferences) PublishDifference(
	ctx context.Context, publication database.PDFDifferencePublication,
) error {
	status := "identical"
	var alias *string
	if publication.ObjectID != nil {
		value := "/_diffs/" + publication.ID + ".pdf"
		alias = &value
		status = "ready"
	}
	if _, err := d.db.Exec(ctx, `UPDATE app.pdf_differences SET status=$2,object_id=$3,error=NULL WHERE id=$1`,
		publication.ID, status, publication.ObjectID); err != nil {
		return err
	}
	if _, err := d.db.Exec(ctx, `UPDATE app.change_record_items SET diff_webdav_path=$2 WHERE pdf_difference_id=$1`,
		publication.ID, alias); err != nil {
		return err
	}
	_, err := d.db.Exec(ctx, `UPDATE app.file_versions SET diff_webdav_path=$2 WHERE pdf_difference_id=$1`,
		publication.ID, alias)
	return err
}

func (d postgresPDFDifferences) ReadyDifferenceObject(
	ctx context.Context, id string,
) (database.ObjectReference, error) {
	var object database.ObjectReference
	err := d.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
		FROM app.pdf_differences d
		JOIN app.objects o ON o.id=d.object_id
		JOIN app.courses c ON c.id=d.course_id AND c.hidden=0
		WHERE d.id=$1 AND d.status='ready'`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID, &object.SHA256, &object.Bytes, &object.MediaType)
	return object, err
}

func (d postgresPDFDifferences) DifferenceStatus(ctx context.Context, id string) (string, error) {
	var status string
	err := d.db.QueryRow(ctx, `SELECT status FROM app.pdf_differences WHERE id=$1`, id).Scan(&status)
	return status, err
}
