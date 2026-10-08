package annotations

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// Snapshot reads marks in an existing snapshot without rewriting their anchors.
// Explicit reanchoring still records orphaning durably through List.
func Snapshot(
	ctx context.Context,
	tx database.Operations,
	course int64,
	document, action string,
	deleted bool,
) ([]Annotation, error) {
	if document != "" {
		action = ""
	}
	rows, err := tx.Annotations().SnapshotAnnotations(ctx, database.ListAnnotationsParams{
		CourseID: course,
		Document: document,
		Action:   identity.Encode(action),
		Deleted:  deleted,
	})
	if err != nil {
		return nil, err
	}
	result := []Annotation{}
	bytes := 0
	for _, row := range rows {
		item, size, err := snapshotItem(row)
		if err != nil {
			return nil, err
		}
		bytes += size
		if len(result) >= 1000 || bytes > 8*1024*1024 {
			return nil, errors.New("workspace annotations exceed the bounded response budget")
		}
		if item.Status == "active" && (row.Current == nil || *row.Current != item.SourceHash) {
			item.Status = "orphaned"
		}
		result = append(result, item)
	}
	return result, nil
}

func snapshotItem(row database.AnnotationSnapshotRow) (Annotation, int, error) {
	stored := row.AnnotationRow
	raw := assembleAnnotation(
		stored.ID, stored.CourseID, stored.PageNumber, stored.DocumentID, stored.SourceHash,
		stored.Kind, stored.Origin, stored.Status, stored.Color, stored.Quote, stored.Prefix,
		stored.Suffix, stored.CharStart, stored.CharEnd, stored.RectsJSON, stored.ChunkID,
		stored.Body, stored.TagsJSON, stored.Action, stored.Unit, stored.Revision,
		stored.Session, stored.Created, stored.Updated,
	)
	item, err := decode(raw)
	if err != nil {
		return Annotation{}, 0, err
	}
	return item, len(raw), nil
}
