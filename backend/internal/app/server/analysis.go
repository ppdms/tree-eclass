package server

import (
	"context"
	"log/slog"
	"time"

	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/services/analysis"
	"tree-eclass/internal/services/synthesis"
)

func (s *Server) analysisService() analysis.Service {
	return analysis.Service{
		Pool:      s.db.Pool,
		Objects:   s.blobs,
		Parser:    s.parser,
		Temp:      s.config.Temp,
		Keys:      s.config.ProviderKeys,
		Generator: inference.Generator{Client: s.inference},
	}
}
func (s *Server) synthesisService() synthesis.Service {
	return synthesis.Service{
		Pool:      s.db.Pool,
		Keys:      s.config.ProviderKeys,
		Generator: inference.Generator{Client: s.inference},
	}
}
func (s *Server) analysisWorker(ctx context.Context) error {
	if !s.config.ExternalWorkers {
		<-ctx.Done()
		return nil
	}
	service := s.analysisService()
	synth := s.synthesisService()
	lane := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if !s.expensiveMu.TryLock() {
			continue
		}
		var err error
		switch lane % 5 {
		case 3:
			_, err = synth.RunOne(ctx, "course")
		case 4:
			_, err = synth.RunOne(ctx, "practice")
		default:
			_, err = service.RunOne(ctx)
		}
		lane++
		s.expensiveMu.Unlock()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			slog.Error("analysis worker", "error", err)
		}
	}
}
