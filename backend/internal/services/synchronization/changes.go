package synchronization

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/integrations/pdfdiff"
)

func saveChanges(ctx context.Context, tx database.Tx, courseID int64, changes []Change) error {
	if len(changes) == 0 {
		return nil
	}
	counts := result(changes)
	zone, err := time.LoadLocation("Europe/Athens")
	if err != nil {
		return err
	}
	changeNo := time.Now().In(zone).Format("2006-01-02T15:04:05.000000000")
	record, err := tx.Sync().InsertChangeRecord(ctx, courseID, changeNo,
		fmt.Sprintf("+ %d − %d ~ %d", counts.Added, counts.Deleted, counts.Modified), len(changes))
	if err != nil {
		return err
	}
	for _, change := range changes {
		difference, alias, err := queueDifference(ctx, tx, courseID, change)
		if err != nil {
			return err
		}
		if err = tx.Sync().InsertChangeHistory(ctx, courseID, change.Type, identity.Encode(change.Path)); err != nil {
			return err
		}
		if err = tx.Sync().InsertChangeRecordItem(ctx, database.SyncChangeRecordItemInput{
			RecordID:   record,
			Type:       change.Type,
			Path:       identity.Encode(change.Path),
			Name:       identity.Encode(change.Name),
			Redirect:   change.Redirect,
			Difference: difference,
			DiffAlias:  alias,
		}); err != nil {
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
	tx database.Tx,
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
	return tx.Sync().InsertFileVersion(ctx, database.SyncFileVersionInput{
		CourseID:    courseID,
		Path:        identity.Encode(change.Path),
		StoragePath: alias,
		Type:        kind,
		Name:        identity.Encode(f.Name),
		Redirect:    f.Redirect,
		Revision:    revision,
		Difference:  difference,
		DiffAlias:   diffAlias,
	})
}

func queueDifference(ctx context.Context, tx database.Tx, course int64, change Change) (*string, *string, error) {
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
