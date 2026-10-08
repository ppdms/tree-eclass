package activity

import (
	"context"
	"encoding/json"
	"errors"

	"tree-eclass/internal/domain/database"
)

type CoursePage struct {
	Timeline []Item `json:"timeline"`
	More     bool   `json:"has_more"`
	Offset   int64  `json:"offset"`
}

func (s Reader) CoursePage(ctx context.Context, course, limit, offset int64) (CoursePage, error) {
	result := CoursePage{Timeline: []Item{}, Offset: offset}
	if limit < 1 || limit > 50 || offset < 0 || offset > 1<<31-1 {
		return result, errors.New("invalid course update page")
	}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = CoursePageTx(ctx, tx, course, limit, offset)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func CoursePageTx(ctx context.Context, tx database.Tx, course, limit, offset int64) (CoursePage, error) {
	result := CoursePage{Timeline: []Item{}, Offset: offset}
	if limit < 1 || limit > 50 || offset < 0 || offset > 1<<31-1 {
		return result, errors.New("invalid course update page")
	}
	events, err := tx.Activity().CoursePage(ctx, course, limit+1, offset)
	if err != nil {
		return result, err
	}
	for _, event := range events {
		if int64(len(result.Timeline)) == limit {
			result.More = true
			break
		}
		item := courseEventItem(course, event)
		item.decode()
		result.Timeline = append(result.Timeline, item)
	}
	return result, nil
}

func courseEventItem(course int64, event database.ActivityCourseEvent) Item {
	raw, _ := json.Marshal(event.ID)
	item := Item{
		Type:      event.Kind,
		ID:        raw,
		Timestamp: event.Timestamp,
		CourseID:  &course,
	}
	if event.Timestamp != nil {
		item.SortKey = *event.Timestamp
	}
	if event.Kind == "change" {
		item.ChangeNo = event.ChangeNo
		item.Message = event.Message
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
