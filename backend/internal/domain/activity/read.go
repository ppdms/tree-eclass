package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"tree-eclass/internal/domain/database"
)

type Reader struct{ Pool database.Store }

func (r Reader) Page(ctx context.Context, limit, offset int64, timeline bool) (Page, error) {
	limit = min(50, max(10, limit))
	offset = max(0, offset)
	result := Page{Groups: []Group{}, Next: offset}
	if offset > 1<<31-1 {
		return result, errors.New("activity offset exceeds the supported range")
	}
	tx, err := r.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = pageTx(ctx, tx, limit, offset, timeline, result)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func pageTx(ctx context.Context, tx database.Tx, limit, offset int64, timeline bool, result Page) (Page, error) {
	events, err := tx.Activity().Page(ctx, limit+1, offset)
	if err != nil {
		return result, err
	}
	result.More = int64(len(events)) > limit
	items := make([]Item, 0, min(len(events), int(limit)))
	for _, event := range events[:min(len(events), int(limit))] {
		item := eventItem(event)
		item.decode()
		items = append(items, item)
	}
	result.Groups = Groups(items)
	result.Next += int64(len(items))
	if timeline {
		result.Timeline = &items
	}
	return result, nil
}

func eventItem(event database.ActivityEvent) Item {
	item := Item{
		Type:       event.Type,
		Timestamp:  event.Timestamp,
		SortKey:    event.SortKey,
		CourseID:   event.CourseID,
		CourseName: event.CourseName,
		ShortName:  event.ShortName,
	}
	if event.Kind == "global" {
		quoted, _ := json.Marshal("global_" + strconv.FormatInt(event.ID, 10))
		item.ID = quoted
	} else {
		raw, _ := json.Marshal(event.ID)
		item.ID = raw
	}
	if event.Type == "change" {
		item.ChangeNo = event.ChangeNo
		item.Message = event.Message
		item.Changes = make([]Change, 0, len(event.Changes))
		for _, change := range event.Changes {
			item.Changes = append(item.Changes, Change{
				Type:     change.ChangeType,
				Path:     change.FilePath,
				Name:     change.Display,
				Redirect: change.Redirect,
				Diff:     change.Diff,
			})
		}
		return item
	}
	if event.Title != nil {
		item.Title = *event.Title
	}
	if event.Link != nil {
		item.Link = *event.Link
	}
	item.Description = event.Desc
	return item
}
