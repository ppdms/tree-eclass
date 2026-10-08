// Package database defines SQL-free persistence contracts and neutral values.
package database

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoRows      = errors.New("database: no rows in result set")
	ErrUnique      = errors.New("database: unique constraint violation")
	ErrForeignKey  = errors.New("database: foreign key constraint violation")
	ErrReadOnly    = errors.New("database: read-only transaction")
	ErrFinished    = errors.New("database: transaction already finished")
	ErrUnavailable = errors.New("database: unavailable")
	ErrFailed      = errors.New("database: transaction requires rollback")
)

func IsNoRows(err error) bool          { return errors.Is(err, ErrNoRows) }
func IsUniqueViolation(err error) bool { return errors.Is(err, ErrUnique) }

type Isolation int

const (
	Default Isolation = iota
	RepeatableRead
	Serializable
)

type AccessMode int

const (
	ReadWrite AccessMode = iota
	ReadOnly
)

type Options struct {
	Isolation  Isolation
	AccessMode AccessMode
}

// OptionalTime is a nullable UTC instant, independent of database driver types.
type OptionalTime struct {
	Time  time.Time
	Valid bool
}

// MarshalJSON preserves nullable timestamp wire values without a driver codec.
func (t OptionalTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.UTC().MarshalJSON()
}

// Iterator streams typed records without exposing SQL rows or positional scanning.
type Iterator[T any] interface {
	Next() bool
	Value() T
	Err() error
	Close()
}

// Tx binds every operation accessor to one atomic database transaction.
// RepeatableRead and Serializable readers retain one stable snapshot.
// Read-only transactions reject mutations; failed SQL makes either backend rollback-only.
type Tx interface {
	Operations
	Commit(context.Context) error
	Rollback(context.Context) error
}

// Store owns the configured backend. Business code receives this port, never SQL handles.
// Close rejects new work, waits for admitted transactions and iterators to finish,
// then releases runtime ownership. Consumers must close iterators and finish transactions.
type Store interface {
	Operations
	Begin(context.Context) (Tx, error)
	BeginTx(context.Context, Options) (Tx, error)
	Ping(context.Context) error
	Close()
}
