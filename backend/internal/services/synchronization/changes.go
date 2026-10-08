package synchronization

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/integrations/pdfdiff"
)

func saveChanges(ctx context.Context, tx rdbms.Tx, courseID int64, changes []Change) error {
	if len(changes) == 0 {
		return nil
	}
	counts := result(changes)
	zone, err := time.LoadLocation("Europe/Athens")
	if err != nil {
		return err
	}
	changeNo := time.Now().In(zone).Format("2006-01-02T15:04:05.000000000")
	var record int64
	err = tx.QueryRow(
		ctx,
		`INSERT INTO app.change_records(course_id,change_no,message,changes_count) VALUES($1,$2,$3,$4) RETURNING id`,
		courseID,
		changeNo,
		fmt.Sprintf("+ %d − %d ~ %d", counts.Added, counts.Deleted, counts.Modified),
		len(changes),
	).
		Scan(&record)
	if err != nil {
		return err
	}
	for _, change := range changes {
		difference, alias, err := queueDifference(ctx, tx, courseID, change)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO app.change_history(course_id,change_type,file_path) VALUES($1,$2,$3)`, courseID, change.Type, identity.Encode(change.Path)); err != nil {
			return err
		}
		if _, err = tx.Exec(
			ctx,
			`INSERT INTO app.change_record_items(change_record_id,change_type,file_path,display_name,redirect_url,pdf_difference_id,diff_webdav_path) VALUES($1,$2,$3,$4,NULLIF($5,''),$6,$7)`,
			record,
			change.Type,
			identity.Encode(change.Path),
			identity.Encode(change.Name),
			change.Redirect,
			difference,
			alias,
		); err != nil {
			return err
		}
		if change.Previous != nil {
			if err = archiveVersion(ctx, tx, courseID, change, difference, alias); err != nil {
				return err
			}
		}
	}
	return announceChanges(ctx, tx, courseID, record, changes)
}

func archiveVersion(
	ctx context.Context,
	tx rdbms.Tx,
	courseID int64,
	change Change,
	difference, diffAlias *string,
) error {
	f := change.Previous
	var revision, alias *string
	if f.Revision != "" {
		revision = &f.Revision
		value := path.Join("/_revisions", f.Revision, path.Base(f.Path))
		alias = &value
	}
	kind := strings.TrimSuffix(change.Type, "_file")
	_, err := tx.Exec(
		ctx,
		`INSERT INTO app.file_versions(course_id,file_path,version_webdav_path,change_type,display_name,redirect_url,revision_id,pdf_difference_id,diff_webdav_path) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9)`,
		courseID,
		identity.Encode(change.Path),
		alias,
		kind,
		identity.Encode(f.Name),
		f.Redirect,
		revision,
		difference,
		diffAlias,
	)
	return err
}

func queueDifference(ctx context.Context, tx rdbms.Tx, course int64, change Change) (*string, *string, error) {
	old, next := change.Previous, change.Current
	if change.Type != "modified_file" || old == nil || next == nil || old.Object == nil || next.Object == nil ||
		!strings.EqualFold(path.Ext(old.Name), ".pdf") ||
		!strings.EqualFold(path.Ext(next.Name), ".pdf") {
		return nil, nil, nil
	}
	if old.Object.SHA256 == next.Object.SHA256 {
		return nil, nil, nil
	}
	id, alias, err := pdfdiff.Enqueue(ctx, tx, course, *old.Object, *next.Object)
	if err != nil {
		return nil, nil, err
	}
	return &id, alias, nil
}
