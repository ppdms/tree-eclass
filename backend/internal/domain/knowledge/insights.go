package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
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
func pagesTx(ctx context.Context, tx rdbms.Tx, course int64, document string, first, last int64) (PageInsights, error) {
	result := PageInsights{DocumentID: document, Pages: []PageInsight{}, Notice: PageNotice, UntrustedContent: true}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	var hash string
	var count *int64
	err = tx.QueryRow(ctx, `SELECT d.source_hash,d.page_count FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id WHERE d.id=$1 AND d.course_id=$2 AND `+CurrentSourcePredicate+` AND d.status='ready' AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=c.id AND p.enabled=1))`, document, course).
		Scan(&hash, &count)
	if err != nil {
		return result, err
	}
	first, last = clampPageRange(first, last, count)
	result.FirstPage, result.LastPage = first, last
	rows, err := tx.Query(
		ctx,
		`SELECT page_number,status,model,generated_at,source_hash,CASE WHEN octet_length(payload_json)<=262144 THEN payload_json END,analysis_version,requested_model FROM knowledge.page_enrichments WHERE document_id=$1 AND page_number BETWEEN $2 AND $3 ORDER BY page_number`,
		document,
		first,
		last,
	)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	if err := appendPageInsights(rows, hash, a.Model, &result); err != nil {
		return result, err
	}
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

func appendPageInsights(rows rdbms.Rows, hash, model string, result *PageInsights) error {
	for rows.Next() {
		var item PageInsight
		var source, version, requested string
		var raw *string
		if err := rows.Scan(
			&item.PageNumber,
			&item.Status,
			&item.Model,
			&item.GeneratedAt,
			&source,
			&raw,
			&version,
			&requested,
		); err != nil {
			return err
		}
		item.Stale = source != hash || version != settings.PageAnalysisVersion || requested != model
		if item.Status == "ready" && raw != nil && !item.Stale {
			item.Insight = pagePayload(*raw)
		}
		result.Pages = append(result.Pages, item)
	}
	return rows.Err()
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	return readDocumentAnalysis(ctx, tx, a, id, hash)
}

func readDocumentAnalysis(ctx context.Context, tx rdbms.Tx, a settings.AI, id, hash string) (map[string]any, error) {
	var status, source, model, requested, version, kind, currentHash string
	var payload, generated *string
	err := tx.QueryRow(ctx, `SELECT e.status,e.source_hash,e.model,coalesce(e.requested_model,e.model),e.analysis_version,CASE WHEN octet_length(e.payload_json)<=1048576 THEN e.payload_json END,e.generated_at,d.document_kind,d.source_hash FROM knowledge.document_enrichments e JOIN knowledge.documents d ON d.id=e.document_id JOIN app.courses c ON c.id=d.course_id WHERE e.document_id=$1 AND d.is_current=1 AND d.status='ready' AND c.hidden=0`, id).
		Scan(&status, &source, &model, &requested, &version, &payload, &generated, &kind, &currentHash)
	if errors.Is(err, rdbms.ErrNoRows) {
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
	stale := source != hash || currentHash != hash || requested != a.Model || version != settings.DocumentVersion(kind)
	if !stale && status == "ready" && payload != nil {
		_ = json.Unmarshal([]byte(*payload), &insight)
	}
	if stale {
		status = "not_queued"
	}
	summary, _ := insight["summary"].(string)
	return map[string]any{
		"status":                      status,
		"ready":                       !stale && status == "ready" && strings.TrimSpace(summary) != "",
		"stale":                       stale,
		"source_hash":                 source,
		"model":                       model,
		"requested_model":             requested,
		"analysis_version":            version,
		"generated_at":                generated,
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
