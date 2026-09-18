package server

import (
	"context"
	"fmt"
	"time"
)

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
		if err != nil {
			return fmt.Errorf("runtime database ownership lost: %w", err)
		}
	}
}
