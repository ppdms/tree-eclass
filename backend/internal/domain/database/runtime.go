package database

import "context"

// Runtime records code identity in the active dataset without leaking its schema.
type Runtime interface {
	SetRuntimeCode(context.Context, string) error
}
