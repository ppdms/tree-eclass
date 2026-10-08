package server

import (
	"context"
	"fmt"
	"time"
)

// Ownership watches the exclusive database lock. Store.Ping probes the
// owning session or locked file identically on every backend, so any probe
// failure means real ownership loss and must stop the server.
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
		return fmt.Errorf("runtime database ownership lost: %w", err)
	}
}
