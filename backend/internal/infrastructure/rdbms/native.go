package rdbms

import (
	"context"
	"fmt"
	"time"

	"tree-eclass/internal/domain/database"
)

// These SQL interfaces are private to backend implementations. Application ports
// expose typed operations instead, and neither backend rewrites another dialect.
type nativeDBTX interface {
	Exec(context.Context, string, ...any) (nativeResult, error)
	Query(context.Context, string, ...any) (nativeRows, error)
	QueryRow(context.Context, string, ...any) nativeRow
}

type nativeResult interface{ RowsAffected() int64 }
type nativeRow interface{ Scan(...any) error }
type nativeRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}

type nativeIterator[T any] struct {
	rows  nativeRows
	scan  func(nativeRows) (T, error)
	value T
	err   error
}

func (i *nativeIterator[T]) Next() bool {
	if i.err != nil || !i.rows.Next() {
		return false
	}
	i.value, i.err = i.scan(i.rows)
	return i.err == nil
}
func (i *nativeIterator[T]) Value() T { return i.value }
func (i *nativeIterator[T]) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.rows.Err()
}
func (i *nativeIterator[T]) Close() { i.rows.Close() }

func typedIterator[T any](rows nativeRows, scan func(nativeRows) (T, error)) database.Iterator[T] {
	return &nativeIterator[T]{rows: rows, scan: scan}
}

type nativeTime database.OptionalTime

func (t *nativeTime) Scan(value any) error {
	if value == nil {
		*t = nativeTime{}
		return nil
	}
	if instant, ok := value.(time.Time); ok {
		*t = nativeTime{Time: instant.UTC(), Valid: true}
		return nil
	}
	text, ok := value.(string)
	if bytes, bytesOK := value.([]byte); bytesOK {
		text, ok = string(bytes), true
	}
	if !ok {
		return fmt.Errorf("database: unexpected timestamp %T", value)
	}
	for _, layout := range []string{
		time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05",
	} {
		if instant, err := time.Parse(layout, text); err == nil {
			*t = nativeTime{Time: instant.UTC(), Valid: true}
			return nil
		}
	}
	return fmt.Errorf("database: invalid UTC timestamp %q", text)
}
