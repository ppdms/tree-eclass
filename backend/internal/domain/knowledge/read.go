package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/storage/queries"
)

type Locator struct {
	Type  string  `json:"type"`
	Start string  `json:"start"`
	End   *string `json:"end,omitempty"`
}
type ReadRequest struct {
	DocumentID       string    `json:"document_id"`
	Locators         []Locator `json:"locators"`
	IncludeNeighbors bool      `json:"include_neighbors"`
	MaxCharacters    int       `json:"max_characters"`
}
type ReadUnit struct {
	ChunkID          string         `json:"chunk_id"`
	Ordinal          int64          `json:"ordinal"`
	LocatorType      string         `json:"locator_type"`
	LocatorStart     *string        `json:"locator_start"`
	LocatorEnd       *string        `json:"locator_end"`
	Heading          *string        `json:"heading"`
	Metadata         map[string]any `json:"metadata"`
	Text             string         `json:"text"`
	UntrustedContent bool           `json:"untrusted_content"`
}
type ReadResponse struct {
	Document               map[string]any   `json:"document"`
	Units                  []ReadUnit       `json:"units"`
	Characters             int              `json:"characters"`
	Truncated              bool             `json:"truncated"`
	PageAnalyses           []map[string]any `json:"page_study_analyses"`
	DerivedInsightNotice   *string          `json:"derived_insight_notice"`
	UntrustedContentNotice string           `json:"untrusted_content_notice"`
}

func (s Reader) Read(ctx context.Context, request ReadRequest) (ReadResponse, error) {
	result := ReadResponse{
		Units:                  []ReadUnit{},
		PageAnalyses:           []map[string]any{},
		UntrustedContentNotice: UntrustedNotice,
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	document, err := resourceDocument(ctx, tx, request.DocumentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrUnavailable
	}
	if err != nil {
		return result, err
	}
	result.Document = documentEvidence(document)
	if document.Status != "ready" {
		return result, nil
	}
	maximum := readLimit(request.MaxCharacters)
	if err := readChunkUnits(ctx, tx, request, maximum, &result); err != nil {
		return result, err
	}
	if err := readPageAnalyses(ctx, tx, document, &result); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func readLimit(maxCharacters int) int {
	maximum := min(max(maxCharacters, 1), 50000)
	if maxCharacters == 0 {
		return 30000
	}
	return maximum
}

func readChunkUnits(ctx context.Context, tx pgx.Tx, request ReadRequest, maximum int, result *ReadResponse) error {
	ordinals, err := readOrdinals(ctx, tx, request)
	if err != nil {
		return err
	}
	rows, err := tx.Query(
		ctx,
		`SELECT id,ordinal,locator_type,locator_start,locator_end,heading,metadata_json,text FROM knowledge.chunks WHERE document_id=$1 AND ($2 OR ordinal=ANY($3::bigint[])) ORDER BY ordinal`,
		request.DocumentID,
		len(request.Locators) == 0,
		ordinals,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		if result.Characters >= maximum {
			result.Truncated = true
			break
		}
		unit, err := nextReadUnit(rows)
		if err != nil {
			rows.Close()
			return err
		}
		chars := []rune(unit.Text)
		remaining := maximum - result.Characters
		if len(chars) > remaining {
			chars = chars[:remaining]
			result.Truncated = true
		}
		unit.Text, unit.UntrustedContent = string(chars), true
		result.Characters += len(chars)
		result.Units = append(result.Units, unit)
	}
	err = rows.Err()
	rows.Close()
	return err
}

func nextReadUnit(rows pgx.Rows) (ReadUnit, error) {
	var unit ReadUnit
	var metadata string
	if err := rows.Scan(
		&unit.ChunkID,
		&unit.Ordinal,
		&unit.LocatorType,
		&unit.LocatorStart,
		&unit.LocatorEnd,
		&unit.Heading,
		&metadata,
		&unit.Text,
	); err != nil {
		return unit, err
	}
	unit.Metadata = map[string]any{}
	if err := json.Unmarshal([]byte(metadata), &unit.Metadata); err != nil {
		return unit, err
	}
	unit.Text = identity.Decode(unit.Text)
	if unit.Heading != nil {
		*unit.Heading = identity.Decode(*unit.Heading)
	}
	return unit, nil
}

func readPageAnalyses(ctx context.Context, tx pgx.Tx, document queries.KnowledgeDocument, result *ReadResponse) error {
	seen := map[string]bool{}
	for _, unit := range result.Units {
		if unit.LocatorType != "page" || unit.LocatorStart == nil || seen[*unit.LocatorStart] {
			continue
		}
		seen[*unit.LocatorStart] = true
		page, err := strconv.ParseInt(*unit.LocatorStart, 10, 64)
		if err != nil || page < 1 {
			continue
		}
		pages, err := pagesTx(ctx, tx, document.CourseID, document.ID, page, page)
		if err != nil {
			return err
		}
		if len(pages.Pages) == 0 {
			continue
		}
		p := pages.Pages[0]
		if !p.Stale && p.Status == "ready" && p.Insight["summary"] != nil {
			result.PageAnalyses = append(
				result.PageAnalyses,
				map[string]any{
					"status":                      p.Status,
					"ready":                       true,
					"page_number":                 page,
					"model":                       p.Model,
					"generated_at":                p.GeneratedAt,
					"insight":                     p.Insight,
					"derived_not_source_evidence": true,
				},
			)
		}
	}
	if len(result.PageAnalyses) > 0 {
		notice := DerivedNotice
		result.DerivedInsightNotice = &notice
	}
	return nil
}
func documentEvidence(d queries.KnowledgeDocument) map[string]any {
	decode := func(value *string) *string {
		if value == nil {
			return nil
		}
		text := identity.Decode(*value)
		return &text
	}
	d.SourceUrl, d.CourseShortName = decode(d.SourceUrl), decode(d.CourseShortName)
	evidence := "official_material"
	if d.SourceOrigin == "external" {
		evidence = "course_material"
	}
	return map[string]any{
		"id":                 d.ID,
		"course_id":          d.CourseID,
		"course_name":        identity.Decode(d.CourseName),
		"course_short_name":  d.CourseShortName,
		"display_name":       identity.Decode(d.DisplayName),
		"source_path":        identity.Decode(d.SourcePath),
		"source_url":         d.SourceUrl,
		"source_hash":        d.SourceHash,
		"document_kind":      d.DocumentKind,
		"academic_year":      d.AcademicYear,
		"source_modified_at": d.SourceModifiedAt,
		"indexed_at":         d.IndexedAt,
		"source_origin":      d.SourceOrigin,
		"evidence_class":     evidence,
	}
}
func readOrdinals(ctx context.Context, tx pgx.Tx, request ReadRequest) ([]int64, error) {
	result := []int64{}
	if len(request.Locators) == 0 {
		return result, nil
	}
	rows, err := tx.Query(
		ctx,
		`SELECT ordinal,locator_type,coalesce(locator_start,'') FROM knowledge.chunks WHERE document_id=$1 ORDER BY ordinal`,
		request.DocumentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	selected := map[int64]bool{}
	for rows.Next() {
		var ordinal int64
		var kind, start string
		if err = rows.Scan(&ordinal, &kind, &start); err != nil {
			return nil, err
		}
		for _, locator := range request.Locators {
			if kind != locator.Type || !includes(locator, start) {
				continue
			}
			selected[ordinal] = true
			if request.IncludeNeighbors {
				if ordinal > 0 {
					selected[ordinal-1] = true
				}
				selected[ordinal+1] = true
			}
		}
	}
	for ordinal := range selected {
		result = append(result, ordinal)
	}
	return result, rows.Err()
}
func includes(locator Locator, value string) bool {
	end := locator.Start
	if locator.End != nil && *locator.End != "" {
		end = *locator.End
	}
	a, ea := strconv.ParseInt(locator.Start, 10, 64)
	b, eb := strconv.ParseInt(end, 10, 64)
	v, ev := strconv.ParseInt(value, 10, 64)
	if ea == nil && eb == nil && ev == nil {
		return a <= v && v <= b
	}
	return locator.Start <= value && value <= end
}
