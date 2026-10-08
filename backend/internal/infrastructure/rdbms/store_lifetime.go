package rdbms

import (
	"sync"

	"tree-eclass/internal/domain/database"
)

// Close stops admission before waiting for checked-out transactions and rows.
// The database owner lock outlives every admitted operation, not just its pool.
// Native pools and transactions are not usable after Close returns.
type storeLifetime struct {
	mu     sync.Mutex
	closed bool
	active sync.WaitGroup
	once   sync.Once
}

func (l *storeLifetime) enter() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return database.ErrFinished
	}
	l.active.Add(1)
	return nil
}

func (l *storeLifetime) leave() { l.active.Done() }

func (l *storeLifetime) close(finish func()) {
	l.once.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
		l.active.Wait()
		finish()
	})
}
