package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
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

// searchSelect lists the explicit columns both search paths scan. The JSON
// blob decodeCandidate reads is assembled in Go (assembleCandidate) so the
// query stays portable: to_jsonb/jsonb_build_object have no sqlite form.
const searchSelect = `d.id,d.course_id,d.course_name,d.course_short_name,d.source_path,` +
	`d.source_origin,d.normalized_path,d.source_url,d.display_name,d.source_hash,d.mime_type,` +
	`d.response_mime_type,d.document_kind,d.academic_year,d.source_modified_at,d.indexed_at,` +
	`c.id,c.ordinal,c.locator_type,c.locator_start,c.locator_end,c.heading,c.metadata_json,` +
	`substr(coalesce(f.text,''),1,400),coalesce(f.text,'')`

// searchRow holds one explicit-column search row; pointers preserve NULLs the
// way to_jsonb nulls did. vector is appended by the semantic path only.
type searchRow struct {
	documentID                                                            string
	courseID                                                              int64
	courseName, sourcePath, sourceOrigin, normalizedPath, displayName     string
	sourceHash, documentKind                                              string
	courseShortName, sourceURL, academicYear, sourceModifiedAt, indexedAt *string
	mimeType, responseMIMEType                                            *string
	chunkID                                                               string
	ordinal                                                               int64
	locatorType                                                           string
	locatorStart, locatorEnd, heading                                     *string
	metadataJSON, excerpt, text                                           string
	vector                                                                []byte
}

func (row *searchRow) pointers(withVector bool) []any {
	pointers := []any{
		&row.documentID, &row.courseID, &row.courseName, &row.courseShortName, &row.sourcePath,
		&row.sourceOrigin, &row.normalizedPath, &row.sourceURL, &row.displayName, &row.sourceHash, &row.mimeType,
		&row.responseMIMEType, &row.documentKind, &row.academicYear, &row.sourceModifiedAt, &row.indexedAt,
		&row.chunkID, &row.ordinal, &row.locatorType, &row.locatorStart, &row.locatorEnd, &row.heading, &row.metadataJSON,
		&row.excerpt, &row.text,
	}
	if withVector {
		pointers = append(pointers, &row.vector)
	}
	return pointers
}

// searchScope builds the portable filter shared by both search paths. Course
// IDs and document kinds expand to IN lists: array parameters (ANY,
// cardinality) have no sqlite form. The folder prefix compares against
// substr(...,length($N)) so the bound parameter stays text-typed on both
// drivers; next is the first free placeholder number.
func searchScope(r SearchRequest, next int, args []any) (string, []any) {
	courses := make([]string, len(r.CourseIDs))
	for i, id := range r.CourseIDs {
		courses[i] = "$" + strconv.Itoa(next+i)
		args = append(args, id)
	}
	next += len(courses)
	clause := CurrentSourcePredicate + ` AND d.status='ready' AND d.course_id IN(` + strings.Join(courses, ",") + `)`
	if len(r.DocumentKinds) > 0 {
		kinds := make([]string, len(r.DocumentKinds))
		for i, kind := range r.DocumentKinds {
			kinds[i] = "$" + strconv.Itoa(next+i)
			args = append(args, kind)
		}
		next += len(kinds)
		clause += ` AND d.document_kind IN(` + strings.Join(kinds, ",") + `)`
	}
	prefix := ""
	if r.FolderPrefix != "" {
		prefix = strings.TrimRight(identity.Path(r.FolderPrefix), "/") + "/"
	}
	clause += ` AND ($` + strconv.Itoa(next) + `='' OR substr(d.normalized_path,1,length($` +
		strconv.Itoa(next) + `))=$` + strconv.Itoa(next) + `)`
	return clause, append(args, prefix)
}

// likeEscape escapes LIKE wildcards in a search term; the matching clauses
// use ESCAPE '\' so literal %/_ in document text match exactly.
func likeEscape(term string) string {
	term = strings.ReplaceAll(term, `\`, `\\`)
	term = strings.ReplaceAll(term, `%`, `\%`)
	return strings.ReplaceAll(term, `_`, `\_`)
}

// lexicalTerms tokenizes the query with the existing search normalizer and
// lowercases for the lower() search_vector maintained on both drivers.
func lexicalTerms(query string) []string {
	terms := strings.Fields(identity.Search(query))
	for i, term := range terms {
		terms[i] = strings.ToLower(term)
	}
	return terms
}

// lexicalClause appends one LIKE per term (all terms must occur) and returns
// the fragment plus its pattern args. The ::text cast targets the postgres
// tsvector; the sqlite driver strips casts so it sees the TEXT search_vector.
func lexicalClause(terms []string, next int) (string, []any) {
	var sb strings.Builder
	args := make([]any, 0, len(terms))
	for i, term := range terms {
		sb.WriteString(" AND f.search_vector::text LIKE $")
		sb.WriteString(strconv.Itoa(next + i))
		sb.WriteString(" ESCAPE '\\'")
		args = append(args, "%"+likeEscape(term)+"%")
	}
	return sb.String(), args
}

// assembleCandidate builds the JSON blob decodeCandidate reads from explicit
// columns. Values are stored encoded (identity.Encode) and decoded inside
// decodeCandidate, matching the old to_jsonb(d)||to_jsonb(c) transport.
func assembleCandidate(row searchRow, score float64) ([]byte, error) {
	return json.Marshal(map[string]any{
		"id":                 row.chunkID,
		"ordinal":            row.ordinal,
		"text":               row.text,
		"metadata_json":      row.metadataJSON,
		"rank":               0,
		"retrieval_score":    score,
		"retrieval_mode":     "",
		"document_id":        row.documentID,
		"course_id":          row.courseID,
		"course_name":        row.courseName,
		"course_short_name":  row.courseShortName,
		"display_name":       row.displayName,
		"source_path":        row.sourcePath,
		"source_url":         row.sourceURL,
		"document_kind":      row.documentKind,
		"academic_year":      row.academicYear,
		"source_modified_at": row.sourceModifiedAt,
		"locator_type":       row.locatorType,
		"locator_start":      row.locatorStart,
		"locator_end":        row.locatorEnd,
		"heading":            row.heading,
		"excerpt":            row.excerpt,
		"metadata_score":     0.0,
		"document_priority":  "",
		"source_origin":      row.sourceOrigin,
		"source_hash":        row.sourceHash,
		"indexed_at":         row.indexedAt,
		"resource_uri":       "",
		"mime_type":          row.mimeType,
		"response_mime_type": row.responseMIMEType,
		"normalized_path":    row.normalizedPath,
	})
}
func (s Reader) lexical(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	terms := lexicalTerms(r.Query)
	if len(terms) == 0 || len(r.CourseIDs) == 0 {
		return []candidate{}, nil
	}
	filter, args := searchScope(r, 1, nil)
	like, patterns := lexicalClause(terms, len(args)+1)
	args = append(args, patterns...)
	args = append(args, limit)
	query := `SELECT ` + searchSelect + ` FROM knowledge.chunks_fts f` +
		` JOIN knowledge.chunks c ON c.id=f.chunk_id JOIN knowledge.documents d ON d.id=c.document_id` +
		` WHERE ` + filter + like + ` ORDER BY d.id,c.ordinal LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	score := -float64(len(terms))
	result := []candidate{}
	for rows.Next() {
		var row searchRow
		if err = rows.Scan(row.pointers(false)...); err != nil {
			return nil, err
		}
		raw, err := assembleCandidate(row, score)
		if err != nil {
			return nil, err
		}
		c, err := decodeCandidate(raw)
		if err != nil {
			return nil, err
		}
		metadataRank(&c, r.Query, false)
		lexical := c.Score
		c.LexicalScore = &lexical
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], false) })
	return result, rows.Err()
}
func (s Reader) semantic(ctx context.Context, r SearchRequest, limit int) ([]candidate, error) {
	filter, args := searchScope(r, 1, nil)
	args = append(args, LocalEmbeddingModel, EmbeddingDimensions)
	filter += ` AND e.model=$` + strconv.Itoa(len(args)-1) + ` AND e.dimensions=$` + strconv.Itoa(len(args))
	rows, err := s.Pool.Query(
		ctx,
		`SELECT `+searchSelect+`,e.vector FROM knowledge.chunk_embeddings e JOIN knowledge.chunks c ON c.id=e.chunk_id JOIN knowledge.documents d ON d.id=c.document_id WHERE `+filter,
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	query := Embed(r.Query)
	result := candidates{}
	for rows.Next() {
		var row searchRow
		if err = rows.Scan(row.pointers(true)...); err != nil {
			return nil, err
		}
		if len(row.vector) != EmbeddingDimensions*4 {
			return nil, errors.New("invalid packed embedding dimensions")
		}
		raw, err := assembleCandidate(row, 0)
		if err != nil {
			return nil, err
		}
		c, err := decodeCandidate(raw)
		if err != nil {
			return nil, err
		}
		score := CosinePacked(query, row.vector)
		c.Score, c.SemanticScore = score, &score
		model := LocalEmbeddingModel
		c.EmbeddingModel = &model
		metadataRank(&c, r.Query, true)
		result.admit(c, limit)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], true) })
	return []candidate(result), rows.Err()
}
