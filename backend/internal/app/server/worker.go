package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func (s *Server) indexWorker(ctx context.Context) error {
	queue := jobs.Queue{Pool: s.db.Pool}
	indexer := knowledge.Indexer{Pool: s.db.Pool, Objects: s.blobs, Temp: s.config.Temp,
		Parser: s.parser}
	indexer.Parser.Tessdata = s.config.Tessdata
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if err := s.indexOnce(ctx, indexer, queue); err != nil {
			return err
		}
	}
}
func (s *Server) indexOnce(ctx context.Context, indexer knowledge.Indexer, queue jobs.Queue) error {
	if !s.expensiveMu.TryLock() {
		return nil
	}
	defer s.expensiveMu.Unlock()
	commands, err := queue.Claim(ctx, "index")
	if err != nil {
		slog.Error("index queue claim", "error", err)
		return nil
	}
	for _, command := range commands {
		err = s.runIndexCommand(ctx, indexer, command)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if failure := queue.Fail(ctx, command, err); failure != nil {
				return failure
			}
		} else if err = queue.Complete(ctx, command.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) runIndexCommand(
	ctx context.Context,
	indexer knowledge.Indexer,
	command queries.AppControlCommand,
) error {
	if command.Action == "pdf_diff" {
		return s.runPDFDifference(ctx, command.Payload)
	}
	if command.Action != "index_document" {
		return s.knowledgeReader().Maintain(ctx, command.Action)
	}
	var payload struct {
		DocumentID string `json:"document_id"`
	}
	if err := json.Unmarshal(command.Payload, &payload); err != nil {
		return err
	}
	if payload.DocumentID == "" {
		return errors.New("invalid document indexing command")
	}
	return indexer.Index(ctx, payload.DocumentID)
}
