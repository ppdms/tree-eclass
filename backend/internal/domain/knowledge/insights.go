package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

const PageNotice = "Page insights are AI-derived reading aids, not source evidence. The page itself is beside them; check it before trusting a claim."

var pageInsightFields = []string{
	"summary",
	"page_type",
	"key_points",
	"definitions",
	"formulas",
	"visuals",
	"examples",
	"assessment_clues",
	"references",
	"low_information",
	"confidence",
	"evidence_mode",
}

type PageInsight struct {
	PageNumber  int64          `json:"page_number"`
	Status      string         `json:"status"`
	Model       string         `json:"model"`
	GeneratedAt *string        `json:"generated_at"`
	Stale       bool           `json:"stale"`
	Insight     map[string]any `json:"insight,omitempty"`
}
type PageInsights struct {
	DocumentID       string        `json:"document_id"`
	FirstPage        int64         `json:"first_page"`
	LastPage         int64         `json:"last_page"`
	Pages            []PageInsight `json:"pages"`
	Notice           string        `json:"notice"`
	UntrustedContent bool          `json:"untrusted_content"`
}

func (s Reader) Pages(ctx context.Context, course int64, document string, first, last int64) (PageInsights, error) {
	result := PageInsights{DocumentID: document, Pages: []PageInsight{}, Notice: PageNotice, UntrustedContent: true}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	value, err := pagesTx(ctx, tx, course, document, first, last)
	if err != nil {
		return result, err
	}
	return value, tx.Commit(ctx)
}

func pagesTx(ctx context.Context, tx database.Tx, course int64, document string, first, last int64) (PageInsights,
	error) {
	result := PageInsights{DocumentID: document, Pages: []PageInsight{}, Notice: PageNotice, UntrustedContent: true}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	hash, count, err := tx.Documents().ReadyDocumentHash(ctx, course, document)
	if err != nil {
		return result, err
	}
	first, last = clampPageRange(first, last, count)
	result.FirstPage, result.LastPage = first, last
	rows, err := tx.Documents().PageEnrichments(ctx, document, first, last, 262144)
	if err != nil {
		return result, err
	}
	appendPageInsights(rows, hash, a.Model, &result)
	return result, nil
}

func clampPageRange(first, last int64, count *int64) (int64, int64) {
	first = max(1, first)
	last = max(first, last)
	if count != nil && *count > 0 {
		last = min(last, *count)
	}
	return first, min(last, first+23)
}

func appendPageInsights(rows []database.PageEnrichment, hash, model string, result *PageInsights) {
	for _, row := range rows {
		item := PageInsight{
			PageNumber: row.PageNumber, Status: row.Status, Model: row.Model, GeneratedAt: row.GeneratedAt,
		}
		item.Stale = row.SourceHash != hash || row.Version != settings.PageAnalysisVersion || row.Requested != model
		if item.Status == "ready" && row.Payload != nil && !item.Stale {
			item.Insight = pagePayload(*row.Payload)
		}
		result.Pages = append(result.Pages, item)
	}
}

func pagePayload(raw string) map[string]any {
	var payload map[string]any
	_ = json.Unmarshal([]byte(raw), &payload)
	result := map[string]any{}
	for _, key := range pageInsightFields {
		value := payload[key]
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case string:
			if v == "" {
				continue
			}
		case []any:
			if len(v) == 0 {
				continue
			}
		case map[string]any:
			if len(v) == 0 {
				continue
			}
		}
		result[key] = value
	}
	return result
}

func (s Reader) documentAnalysis(ctx context.Context, id, hash string) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	analysis, err := readDocumentAnalysis(ctx, tx, a, id, hash)
	if err != nil {
		return nil, err
	}
	return analysis, tx.Commit(ctx)
}

func readDocumentAnalysis(ctx context.Context, tx database.Tx, a settings.AI, id, hash string) (map[string]any, error) {
	row, err := tx.Documents().DocumentAnalysis(ctx, id)
	if errors.Is(err, database.ErrNoRows) {
		return map[string]any{
			"status":                      "not_queued",
			"ready":                       false,
			"untrusted_content":           true,
			"derived_not_source_evidence": true,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	insight := map[string]any{}
	stale := row.SourceHash != hash || row.CurrentHash != hash || row.Requested != a.Model ||
		row.Version != settings.DocumentVersion(row.DocumentKind)
	if !stale && row.Status == "ready" && row.Payload != nil {
		_ = json.Unmarshal([]byte(*row.Payload), &insight)
	}
	status := row.Status
	if stale {
		status = "not_queued"
	}
	summary, _ := insight["summary"].(string)
	return map[string]any{
		"status":                      status,
		"ready":                       !stale && status == "ready" && strings.TrimSpace(summary) != "",
		"stale":                       stale,
		"source_hash":                 row.SourceHash,
		"model":                       row.Model,
		"requested_model":             row.Requested,
		"analysis_version":            row.Version,
		"generated_at":                row.GeneratedAt,
		"insight":                     insight,
		"untrusted_content":           true,
		"derived_not_source_evidence": true,
	}, nil
}

func (s Reader) pageSearchAnalysis(ctx context.Context, c candidate) (map[string]any, error) {
	if c.LocatorType != "page" || c.LocatorStart == nil {
		return nil, nil
	}
	page, err := strconv.ParseInt(*c.LocatorStart, 10, 64)
	if err != nil || page < 1 {
		return nil, nil
	}
	result, err := s.Pages(ctx, c.CourseID, c.DocumentID, page, page)
	if err != nil {
		return nil, err
	}
	if len(result.Pages) == 0 || result.Pages[0].Stale {
		return nil, nil
	}
	entry := result.Pages[0]
	return map[string]any{
		"status":                      entry.Status,
		"ready":                       entry.Status == "ready" && entry.Insight["summary"] != nil,
		"page_number":                 page,
		"model":                       entry.Model,
		"generated_at":                entry.GeneratedAt,
		"insight":                     entry.Insight,
		"derived_not_source_evidence": true,
	}, nil
}
