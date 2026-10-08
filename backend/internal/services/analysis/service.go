package analysis

import (
	"context"
	"errors"
	"time"
	"tree-eclass/internal/integrations/inference"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
	"tree-eclass/internal/integrations/parser"
)

type Service struct {
	Pool      rdbms.Pool
	Objects   *blob.Store
	Parser    *parser.Runner
	Temp      string
	Generator inference.Generator
	Keys      map[string]string
}
type document struct {
	ID, Hash, Kind, Name, CourseName, Path, Origin string
	Course, Pages                                  int64
	Context                                        string
}
type job struct {
	Document                  document
	Page, Attempts            int64
	Claim, Version, Requested string
	AI                        settings.AI
}

func (d document) metadata() map[string]any {
	return map[string]any{
		"document_id":   d.ID,
		"course_id":     d.Course,
		"course_name":   identity.Decode(d.CourseName),
		"display_name":  identity.Decode(d.Name),
		"source_path":   identity.Decode(d.Path),
		"document_kind": d.Kind,
		"source_origin": d.Origin,
		"source_hash":   d.Hash,
	}
}
func (s Service) Recover(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, table := range []string{"document_enrichments", "page_enrichments"} {
		if _, err = tx.Exec(ctx, `UPDATE knowledge.`+table+` SET status='pending',claimed_at=NULL,attempts=greatest(0,attempts-1),available_at=$1 WHERE status='running'`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s Service) RunOne(ctx context.Context) (bool, error) {
	selected, err := s.claim(ctx)
	if errors.Is(err, rdbms.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	result, err := s.generate(ctx, selected)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if err != nil {
		return true, s.fail(ctx, selected, err)
	}
	return true, s.publish(ctx, selected, result)
}
