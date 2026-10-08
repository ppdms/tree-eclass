package knowledge

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

var ErrUnavailable = errors.New("course or document is unavailable")

func (s Reader) Visible(ctx context.Context, requested []int64) ([]int64, error) {
	visible, err := s.Pool.Documents().VisibleCourseIDs(ctx)
	if err != nil {
		return nil, err
	}
	allowed := map[int64]bool{}
	for _, id := range visible {
		allowed[id] = true
	}
	if len(requested) == 0 {
		return visible, nil
	}
	result := []int64{}
	seen := map[int64]bool{}
	for _, id := range requested {
		if !allowed[id] {
			return nil, ErrUnavailable
		}
		if !seen[id] {
			result = append(result, id)
			seen[id] = true
		}
	}
	return result, nil
}

func (s Reader) Search(ctx context.Context, request SearchRequest) (SearchResponse, error) {
	result := SearchResponse{
		Results:                []SearchResult{},
		DocumentDiversity:      true,
		DerivedInsightNotice:   DerivedNotice,
		UntrustedContentNotice: UntrustedNotice,
	}
	if err := request.Validate(); err != nil {
		return result, err
	}
	ids, err := s.Visible(ctx, request.CourseIDs)
	if err != nil {
		return result, err
	}
	request.CourseIDs = ids
	maximum := 20
	if len(ids) > 1 {
		maximum = 12
	}
	request.Limit = min(max(1, request.Limit), maximum)
	result.Query, result.LimitApplied, result.CrossCourseCompacted = request.Query, request.Limit, len(ids) > 1
	if len(ids) == 0 {
		return result, nil
	}
	limit := request.Limit
	if request.Mode == "hybrid" {
		limit = max(limit*8, 40)
	}
	selected, err := s.searchCandidates(ctx, request, limit)
	if err != nil {
		return result, err
	}
	seen := map[string]bool{}
	for _, c := range selected {
		if seen[c.DocumentID] {
			continue
		}
		seen[c.DocumentID] = true
		item, err := s.decorate(ctx, c, request.Mode, len(result.Results)+1)
		if err != nil {
			return result, err
		}
		result.Results = append(result.Results, item.SearchResult)
		if len(result.Results) >= request.Limit {
			break
		}
	}
	return result, nil
}

func (s Reader) searchCandidates(ctx context.Context, request SearchRequest, limit int) ([]candidate, error) {
	var lexical, semantic []candidate
	var err error
	if request.Mode != "semantic" {
		if lexical, err = s.lexical(ctx, request, limit); err != nil {
			return nil, err
		}
	}
	if request.Mode != "lexical" {
		if semantic, err = s.semantic(ctx, request, limit); err != nil {
			return nil, err
		}
	}
	switch request.Mode {
	case "semantic":
		return semantic, nil
	case "hybrid":
		return fuse(lexical, semantic), nil
	default:
		return lexical, nil
	}
}

func (s Reader) decorate(ctx context.Context, c candidate, mode string, rank int) (candidate, error) {
	c.Rank, c.Mode, c.UntrustedContent = rank, mode, true
	c.EvidenceClass = "official_material"
	if c.SourceOrigin == "external" {
		c.EvidenceClass = "course_material"
	}
	locator := ""
	if c.LocatorStart != nil {
		locator = *c.LocatorStart
	}
	c.ResourceURI = "eclass://documents/" + c.DocumentID + "/units/" + c.LocatorType + ":" + url.PathEscape(locator)
	if c.Excerpt == "" {
		c.Excerpt = c.Text
	}
	var err error
	if c.StudyAnalysis, err = s.documentAnalysis(ctx, c.DocumentID, c.SourceHash); err != nil {
		return c, err
	}
	c.PageStudyAnalysis, err = s.pageSearchAnalysis(ctx, c)
	return c, err
}

// searchFilter builds the typed admission filter shared by both search paths.
func searchFilter(r SearchRequest) database.DocumentFilter {
	prefix := ""
	if r.FolderPrefix != "" {
		prefix = strings.TrimRight(identity.Path(r.FolderPrefix), "/") + "/"
	}
	return database.DocumentFilter{CourseIDs: r.CourseIDs, DocumentKinds: r.DocumentKinds, FolderPrefix: prefix}
}

// lexicalTerms uses the same Unicode normalizer as candidate matching.
func lexicalTerms(query string) []string {
	return strings.Fields(identity.Search(query))
}

func (s Reader) lexical(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	terms := lexicalTerms(r.Query)
	if len(terms) == 0 || len(r.CourseIDs) == 0 {
		return []candidate{}, nil
	}
	rows, err := s.Pool.Documents().LexicalCandidates(ctx, searchFilter(r), terms, limit)
	if err != nil {
		return nil, err
	}
	score := -float64(len(terms))
	result := []candidate{}
	for _, row := range rows {
		c, err := candidateFromRow(row, score)
		if err != nil {
			return nil, err
		}
		metadataRank(&c, r.Query, false)
		lexical := c.Score
		c.LexicalScore = &lexical
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], false) })
	return result, nil
}

func (s Reader) semantic(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	iterator, err := s.Pool.Documents().EmbeddedCandidates(ctx, searchFilter(r), LocalEmbeddingModel, EmbeddingDimensions)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	query := Embed(r.Query)
	result := candidates{}
	for iterator.Next() {
		row := iterator.Value()
		if len(row.Vector) != EmbeddingDimensions*4 {
			return nil, errors.New("invalid packed embedding dimensions")
		}
		c, err := candidateFromRow(row.SearchCandidate, 0)
		if err != nil {
			return nil, err
		}
		score := CosinePacked(query, row.Vector)
		c.Score, c.SemanticScore = score, &score
		model := LocalEmbeddingModel
		c.EmbeddingModel = &model
		metadataRank(&c, r.Query, true)
		result.admit(c, limit)
	}
	if err := iterator.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], true) })
	return []candidate(result), nil
}
