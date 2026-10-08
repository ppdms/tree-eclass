package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"tree-eclass/internal/infrastructure/rdbms"
)

// Ownership watches the exclusive database lock. Postgres probes the lock on
// a dedicated owner connection, so a failure means a real takeover and must
// stop the server. Sqlite serializes all work on one connection: a slow
// projection build or upstream crawl can hold it past the probe timeout
// without any takeover. Killing the server for that turns every slow query
// into a crash loop, so sqlite only logs and keeps serving.
func (s *Server) watchOwnership(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.db.CheckOwner(probe)
		cancel()
		if err == nil {
			continue
		}
		if _, ok := rdbms.UnwrapPostgres(s.db.Pool); ok {
			return fmt.Errorf("runtime database ownership lost: %w", err)
		}
		slog.Warn("database probe slow; continuing without ownership check", "error", err)
	}
}
