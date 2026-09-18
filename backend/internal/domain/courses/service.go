// Package courses owns course identity and shelf mutations.
package courses

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/storage/queries"
)

type Service struct{ Pool *pgxpool.Pool }
type Course struct {
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	StoragePrefix string  `json:"storage_prefix"`
	Hidden        bool    `json:"hidden"`
	ShortName     *string `json:"short_name"`
	SortOrder     *int64  `json:"sort_order"`
}

func course(row queries.AppCourse) Course {
	if row.ShortName != nil {
		text := identity.Decode(*row.ShortName)
		row.ShortName = &text
	}
	return Course{
		row.ID,
		identity.Decode(row.Name),
		identity.Decode(row.WebdavFolder),
		row.Hidden != 0,
		row.ShortName,
		row.SortOrder,
	}
}
func (s Service) List(ctx context.Context, hidden bool) ([]Course, error) {
	rows, err := queries.New(s.Pool).ListCourses(ctx, hidden)
	if err != nil {
		return nil, err
	}
	result := make([]Course, 0, len(rows))
	for _, row := range rows {
		result = append(result, course(row))
	}
	return result, nil
}
func (s Service) Get(ctx context.Context, id int64) (Course, error) {
	row, err := queries.New(s.Pool).Course(ctx, id)
	return course(row), err
}

func (s Service) Add(ctx context.Context, id int64, name string) error {
	name = strings.TrimSpace(name)
	if id < 1 || name == "" {
		return errors.New("course ID and name are required")
	}
	return s.mutate(ctx, func(q *queries.Queries) error {
		return q.AddCourse(
			ctx,
			queries.AddCourseParams{ID: id, Name: identity.Encode(name), WebdavFolder: fmt.Sprintf("/Courses/%d", id)},
		)
	})
}
func (s Service) Rename(ctx context.Context, id int64, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("course name is required")
	}
	return s.mutate(ctx, func(q *queries.Queries) error {
		n, err := q.RenameCourse(ctx, queries.RenameCourseParams{ID: id, Name: identity.Encode(name)})
		if err == nil && n == 0 {
			return pgx.ErrNoRows
		}
		return err
	})
}
func (s Service) Hide(ctx context.Context, id int64, hidden bool) error {
	flag := int64(0)
	if hidden {
		flag = 1
	}
	return s.mutate(ctx, func(q *queries.Queries) error {
		n, err := q.HideCourse(ctx, queries.HideCourseParams{ID: id, Hidden: flag})
		if err == nil && n == 0 {
			return pgx.ErrNoRows
		}
		return err
	})
}
func (s Service) mutate(ctx context.Context, fn func(*queries.Queries) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = fn(queries.New(tx)); err != nil {
		return err
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s Service) Reorder(ctx context.Context, ids []int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "LOCK TABLE app.courses IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return err
	}
	q := queries.New(tx)
	visible, err := q.ListCourses(ctx, false)
	if err != nil {
		return err
	}
	expected := map[int64]bool{}
	for _, course := range visible {
		expected[course.ID] = true
	}
	if len(ids) == 0 || len(ids) != len(expected) {
		return errors.New("order must contain each visible course exactly once")
	}
	for index, id := range ids {
		if !expected[id] {
			return errors.New("order must contain each visible course exactly once")
		}
		delete(expected, id)
		if err = q.OrderCourse(ctx, queries.OrderCourseParams{ID: id, SortOrder: new(int64(index))}); err != nil {
			return err
		}
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
