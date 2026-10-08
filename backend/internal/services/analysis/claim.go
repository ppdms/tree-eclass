package analysis

import (
	"context"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

func (s Service) claim(ctx context.Context) (job, error) {
	var j job
	a, err := settings.ReadAI(ctx, s.Pool)
	if err != nil {
		return j, err
	}
	if !a.EnrichmentEnabled {
		return j, database.ErrNoRows
	}
	// Inspect candidates without holding a lock across I/O. Recheck under the
	// course/document locks, then commit the claim before calling a provider.
	selected, err := s.Pool.Analysis().FindCandidate(ctx, a.Model, analysisVersions())
	if err != nil {
		return j, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return j, err
	}
	defer tx.Rollback(ctx)
	d, a, err := s.recheckEligibility(ctx, tx, selected.CourseID, selected.DocumentID)
	if err != nil {
		return j, err
	}
	j = job{
		Document:  d,
		Requested: a.Model,
		Version:   settings.DocumentVersion(d.Kind),
		AI:        a,
		Claim:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err = tx.Analysis().QueueDocument(ctx, database.QueueDocumentParams{
		DocumentID:  j.Document.ID,
		SourceHash:  j.Document.Hash,
		ContextHash: j.Document.Context,
		Version:     j.Version,
		Model:       j.Requested,
		AvailableAt: j.Claim,
	}); err != nil {
		return j, err
	}
	if handled, err := s.claimVisual(ctx, tx, &j, d); handled || err != nil {
		return j, err
	}
	attempts, err := tx.Analysis().ClaimDocument(ctx, selected.DocumentID, j.Claim)
	if err != nil {
		return j, err
	}
	j.Attempts = attempts
	return j, tx.Commit(ctx)
}

func (s Service) recheckEligibility(ctx context.Context, tx database.Tx, course int64,
	id string) (document, settings.AI, error) {
	var d document
	if err := tx.Analysis().LockCourseForClaim(ctx, course); err != nil {
		return d, settings.AI{}, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return d, a, err
	}
	if !a.EnrichmentEnabled {
		return d, a, database.ErrNoRows
	}
	current, err := tx.Analysis().CurrentDocument(ctx, id, true)
	if err != nil {
		return d, a, err
	}
	return fromAnalysisDocument(current), a, nil
}

func (s Service) claimVisual(ctx context.Context, tx database.Tx, j *job, d document) (bool, error) {
	if d.Kind != "pdf" && d.Kind != "image" {
		return false, nil
	}
	if d.Pages < 1 || d.Pages > 12000 {
		if err := tx.Analysis().FailDocumentPageRange(ctx, d.ID); err != nil {
			return true, err
		}
		if err := tx.Commit(ctx); err != nil {
			return true, err
		}
		return true, database.ErrNoRows
	}
	page, attempts, err := claimPage(ctx, tx, *j)
	if errors.Is(err, database.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return true, commitErr
		}
		return true, err
	}
	if err != nil {
		return true, err
	}
	j.Page, j.Attempts = page, attempts
	if page > 0 {
		j.Version = settings.PageAnalysisVersion
		return true, tx.Commit(ctx)
	}
	return false, nil
}

func claimPage(ctx context.Context, tx database.Tx, j job) (int64, int64, error) {
	if err := tx.Analysis().QueuePages(ctx, database.QueuePagesParams{
		DocumentID:  j.Document.ID,
		SourceHash:  j.Document.Hash,
		Version:     settings.PageAnalysisVersion,
		Model:       j.Requested,
		Pages:       j.Document.Pages,
		AvailableAt: j.Claim,
	}); err != nil {
		return 0, 0, err
	}
	claimed, err := tx.Analysis().ClaimPage(ctx, j.Document.ID, j.Claim)
	if err == nil {
		return claimed.Page, claimed.Attempts, nil
	}
	if !errors.Is(err, database.ErrNoRows) {
		return 0, 0, err
	}
	ready, err := tx.Analysis().ReadyPageCount(ctx, j.Document.ID, j.Document.Hash,
		j.Requested, settings.PageAnalysisVersion, j.Document.Pages)
	if err != nil {
		return 0, 0, err
	}
	if ready != j.Document.Pages {
		return 0, 0, database.ErrNoRows
	}
	return 0, 0, nil
}
