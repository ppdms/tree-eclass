package analysis

import (
	"context"
	"errors"
	"strings"

	"tree-eclass/internal/integrations/inference"
)

func (s Service) generate(ctx context.Context, j job) (inference.Generated, error) {
	excerpt, err := s.excerpt(ctx, j)
	if err != nil {
		return inference.Generated{}, err
	}
	if j.Page > 0 {
		return s.generatePage(ctx, j, excerpt)
	}
	fromPages := j.Document.Kind == "pdf" || j.Document.Kind == "image"
	if fromPages {
		excerpt, err = s.pageEvidence(ctx, j)
		if err != nil {
			return inference.Generated{}, err
		}
	}
	if strings.TrimSpace(excerpt) == "" {
		return inference.Generated{}, errors.New("document has no extracted source evidence")
	}
	return s.Generator.Generate(
		ctx,
		inference.AnalysisCandidates(j.AI, s.Keys, "document"),
		documentPrompt(j.Document.metadata(), excerpt, fromPages),
		validateDocument,
	)
}
func (s Service) generatePage(ctx context.Context, j job, excerpt string) (inference.Generated, error) {
	candidates := inference.AnalysisCandidates(j.AI, s.Keys, "page")
	if len(candidates) == 0 {
		return inference.Generated{}, inference.Paused{}
	}
	image, renderErr := s.pageImage(ctx, j)
	var lastErr error
	if renderErr == nil {
		result, err := s.Generator.Generate(
			ctx,
			candidates,
			pagePrompt(j.Document.metadata(), j.Page, j.Document.Pages, excerpt, &image),
			validatePage,
		)
		if err == nil {
			result.Payload["evidence_mode"] = "rendered_page"
			return result, nil
		}
		lastErr = err
		var paused inference.Paused
		if errors.As(err, &paused) || ctx.Err() != nil {
			return inference.Generated{}, err
		}
	}
	if strings.TrimSpace(excerpt) == "" {
		return inference.Generated{}, errors.New("visual page has no usable image or extracted text")
	}
	// A text fallback explicitly records its reduced evidence. It cannot claim
	// unseen diagrams or layout, and never runs for a provider-wide quota pause.
	result, err := s.Generator.Generate(
		ctx,
		candidates,
		pagePrompt(j.Document.metadata(), j.Page, j.Document.Pages, excerpt, nil),
		validatePage,
	)
	if err == nil {
		result.Payload["evidence_mode"] = "extracted_text"
		result.Payload["visuals"] = []string{}
		return result, nil
	}
	return inference.Generated{}, errors.Join(err, lastErr, renderErr)
}
