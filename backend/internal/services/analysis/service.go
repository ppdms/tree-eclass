package analysis

import (
	"context"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/integrations/parser"
)

type Service struct {
	Pool      database.Store
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

// contextHash renders the canonical identity hash for the stored tuple so
// staleness checks compare equal on both backends without SQL-side hashing.
func (d document) contextHash() string {
	return database.AnalysisContextHash(d.ID, d.Course, d.Hash, d.Kind, d.Name, d.CourseName, d.Path, d.Origin)
}

func fromAnalysisDocument(d database.AnalysisDocument) document {
	out := document{
		ID: d.ID, Hash: d.SourceHash, Kind: d.Kind, Name: d.Name,
		CourseName: d.CourseName, Path: d.Path, Origin: d.Origin,
		Course: d.CourseID, Pages: d.Pages,
	}
	out.Context = out.contextHash()
	return out
}

func analysisVersions() database.AnalysisVersions {
	return database.AnalysisVersions{
		Document:  settings.DocumentAnalysisVersion,
		Synthesis: settings.PageSynthesisVersion,
		Page:      settings.PageAnalysisVersion,
	}
}

func (s Service) Recover(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Analysis().RecoverLane(ctx, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s Service) RunOne(ctx context.Context) (bool, error) {
	selected, err := s.claim(ctx)
	if errors.Is(err, database.ErrNoRows) {
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
