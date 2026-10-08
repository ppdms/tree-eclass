package messages

import (
	"context"
	"slices"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/knowledge"
)

func visible(ctx context.Context, tx database.Tx, requested []int64) ([]int64, error) {
	ids, err := tx.Community().VisibleCourseIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(requested) == 0 {
		return ids, nil
	}
	result := []int64{}
	for _, id := range requested {
		if !slices.Contains(ids, id) {
			return nil, knowledge.ErrUnavailable
		}
		if !slices.Contains(result, id) {
			result = append(result, id)
		}
	}
	slices.Sort(result)
	return result, nil
}

type CourseStatus struct {
	CourseID      int64   `json:"course_id"`
	Messages      int64   `json:"messages"`
	Conversations int64   `json:"conversations"`
	Sources       int64   `json:"sources"`
	FailedSources int64   `json:"failed_sources"`
	Latest        *string `json:"latest_message_at"`
}
type Status struct {
	Courses   []CourseStatus   `json:"courses"`
	Totals    map[string]int64 `json:"totals"`
	Mapped    []int64          `json:"mapped_courses"`
	Available bool             `json:"available"`
	Latest    *string          `json:"archive_indexed_through"`
	Notice    string           `json:"untrusted_content_notice"`
}

func (s Reader) Status(ctx context.Context, requested []int64) (Status, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return Status{}, err
	}
	defer tx.Rollback(ctx)
	ids, err := visible(ctx, tx, requested)
	if err != nil {
		return Status{}, err
	}
	result, err := statusTx(ctx, tx, ids)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func statusTx(ctx context.Context, tx database.Tx, ids []int64) (Status, error) {
	result := Status{
		Courses: []CourseStatus{},
		Totals:  map[string]int64{"messages": 0, "conversations": 0, "sources": 0, "failed_sources": 0},
		Mapped:  []int64{},
		Notice:  CommunityNotice,
	}
	rows, err := tx.Community().CourseStatus(ctx, ids)
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		c := CourseStatus{
			CourseID:      row.CourseID,
			Messages:      row.Messages,
			Conversations: row.Conversations,
			Sources:       row.Sources,
			FailedSources: row.FailedSources,
			Latest:        row.Latest,
		}
		result.Courses = append(result.Courses, c)
		result.Mapped = append(result.Mapped, c.CourseID)
		result.Totals["messages"] += c.Messages
		result.Totals["conversations"] += c.Conversations
		result.Totals["sources"] += c.Sources
		result.Totals["failed_sources"] += c.FailedSources
		if c.Latest != nil && (result.Latest == nil || *c.Latest > *result.Latest) {
			result.Latest = c.Latest
		}
	}
	result.Available = len(result.Mapped) > 0
	return result, nil
}
