package knowledge

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strings"

	"tree-eclass/internal/domain/identity"
)

var ErrUnavailable = errors.New("course or document is unavailable")

func (s Reader) Visible(ctx context.Context, requested []int64) ([]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id FROM app.courses WHERE hidden=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	visible := []int64{}
	allowed := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		visible = append(visible, id)
		allowed[id] = true
	}
	if err = rows.Err(); err != nil {
		return nil, err
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

const searchColumns = `to_jsonb(d)||to_jsonb(c)||jsonb_build_object('document_id',d.id)`
const searchFilter = CurrentSourcePredicate + ` AND d.status='ready' AND d.course_id=ANY($1::bigint[])
 AND (cardinality($2::text[])=0 OR d.document_kind=ANY($2::text[])) AND ($3='' OR starts_with(d.normalized_path,$3))`

func searchArgs(r SearchRequest) []any {
	kinds := r.DocumentKinds
	if kinds == nil {
		kinds = []string{}
	}
	prefix := ""
	if r.FolderPrefix != "" {
		prefix = strings.TrimRight(identity.Path(r.FolderPrefix), "/") + "/"
	}
	return []any{r.CourseIDs, kinds, prefix}
}
func (s Reader) lexical(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	tokens := strings.Fields(identity.Search(r.Query))
	for i, token := range tokens {
		tokens[i] = `"` + strings.ReplaceAll(token, `"`, `""`) + `"`
	}
	args := append(searchArgs(r), strings.Join(tokens, " OR "), limit)
	rows, err := s.Pool.Query(ctx, `WITH q AS (SELECT public.tree_query($4) query)
 SELECT `+searchColumns+`||jsonb_build_object('retrieval_score',-ts_rank_cd(f.search_vector,q.query),'excerpt',ts_headline('public.tree_search',f.text,q.query,'StartSel=[, StopSel=], MaxWords=40, MinWords=15'))
 FROM q CROSS JOIN knowledge.chunks_fts f JOIN knowledge.chunks c ON c.id=f.chunk_id JOIN knowledge.documents d ON d.id=c.document_id
 WHERE `+searchFilter+` AND f.search_vector@@q.query ORDER BY -ts_rank_cd(f.search_vector,q.query),d.id,c.ordinal LIMIT $5`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []candidate{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		c, err := decodeCandidate(raw)
		if err != nil {
			return nil, err
		}
		metadataRank(&c, r.Query, false)
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], false) })
	return result, rows.Err()
}
func (s Reader) semantic(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	rows, err := s.Pool.Query(
		ctx,
		`SELECT `+searchColumns+`,e.vector FROM knowledge.chunk_embeddings e JOIN knowledge.chunks c ON c.id=e.chunk_id JOIN knowledge.documents d ON d.id=c.document_id WHERE `+searchFilter+` AND e.model=$4 AND e.dimensions=$5`,
		append(searchArgs(r), LocalEmbeddingModel, EmbeddingDimensions)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	query := Embed(r.Query)
	result := candidates{}
	for rows.Next() {
		var raw, vector []byte
		if err = rows.Scan(&raw, &vector); err != nil {
			return nil, err
		}
		if len(vector) != EmbeddingDimensions*4 {
			return nil, errors.New("invalid packed embedding dimensions")
		}
		c, err := decodeCandidate(raw)
		if err != nil {
			return nil, err
		}
		score := CosinePacked(query, vector)
		c.Score, c.SemanticScore = score, &score
		model := LocalEmbeddingModel
		c.EmbeddingModel = &model
		metadataRank(&c, r.Query, true)
		result.admit(c, limit)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], true) })
	return []candidate(result), rows.Err()
}
