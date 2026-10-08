package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"tree-eclass/internal/infrastructure/rdbms"
	"unicode/utf8"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
)

const sourceFrom = ` FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id
 WHERE d.course_id=$1 AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped') AND ` + knowledge.CurrentSourcePredicate

// readySource filters enrichment rows whose insight payload is usable. The
// ->> lookups run natively on both engines (sqlite evaluates ->/->> over
// TEXT JSON columns the same way postgres does over jsonb); the IS JSON
// OBJECT guard from the legacy query has no sqlite form, so object-ness is
// checked by comparing the first non-space byte to '{' via ltrim.
const readySource = `d.status='ready' AND d.content_hash_verified=1 AND e.status='ready' AND e.source_hash=d.source_hash
 AND coalesce(e.requested_model,e.model)=$2 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END
 AND substr(ltrim(e.payload_json),1,1)='{' AND octet_length(e.payload_json)<=1048576
 AND coalesce((CASE WHEN substr(ltrim(e.payload_json),1,1)='{' THEN e.payload_json::jsonb->>'summary' ELSE '' END),'')<>'' AND coalesce((CASE WHEN substr(ltrim(e.payload_json),1,1)='{' THEN e.payload_json::jsonb->>'course_alignment' ELSE '' END),'')<>'mismatch'`

type documentEvidence struct{ Entry, Snapshot map[string]any }

func collectDocuments(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	a settings.AI,
) ([]documentEvidence, int64, int64, error) {
	args := []any{course, a.Model, settings.DocumentAnalysisVersion, settings.PageSynthesisVersion}
	var total, ready int64
	err := tx.QueryRow(ctx, `SELECT count(*),count(CASE WHEN `+readySource+` THEN 1 END)`+sourceFrom, args...).
		Scan(&total, &ready)
	if err != nil {
		return nil, 0, 0, err
	}
	threshold := min(total, max(int64(3), min(int64(8), (total+19)/20)))
	if ready == 0 || ready < threshold {
		return nil, total, ready, nil
	}
	rows, err := tx.Query(ctx, `SELECT d.id,d.source_hash,d.display_name,d.source_path,d.source_origin,d.document_kind,
 e.source_hash,e.analysis_version,e.model,coalesce(e.requested_model,e.model),e.context_hash,e.payload_json`+sourceFrom+` AND `+readySource+`
 ORDER BY CASE coalesce((CASE WHEN substr(ltrim(e.payload_json),1,1)='{' THEN e.payload_json::jsonb->>'importance' ELSE '' END),'') WHEN 'essential' THEN 0 WHEN 'useful' THEN 1 ELSE 2 END,d.id LIMIT 100`, args...)
	if err != nil {
		return nil, total, ready, err
	}
	defer rows.Close()
	result := []documentEvidence{}
	budget := 0
	for rows.Next() {
		item, err := readDocument(ctx, tx, rows)
		if err != nil {
			return nil, total, ready, err
		}
		raw, err := json.Marshal(item.Entry)
		if err != nil {
			return nil, total, ready, err
		}
		size := utf8.RuneCount(raw)
		if budget+size > 110000 {
			break
		}
		budget += size
		result = append(result, item)
	}
	return result, total, ready, rows.Err()
}

// readDocument scans one explicit-column document row, then collects up to
// three representative chunk excerpts (first, middle, last) with a second
// bounded query ordered by ordinal. The legacy jsonb_agg(to_jsonb(x) ORDER
// BY ordinal) subselect has no portable form: its row constructor fails at
// prepare time on sqlite, so rows assemble in Go instead.
func readDocument(ctx context.Context, tx rdbms.Tx, row rdbms.Rows) (documentEvidence, error) {
	var id, hash, name, path, origin, kind, analysisHash, version, model, requested, contextHash, payload string
	err := row.Scan(
		&id,
		&hash,
		&name,
		&path,
		&origin,
		&kind,
		&analysisHash,
		&version,
		&model,
		&requested,
		&contextHash,
		&payload,
	)
	if err != nil {
		return documentEvidence{}, err
	}
	excerpts, err := collectExcerpts(ctx, tx, id)
	if err != nil {
		return documentEvidence{}, err
	}
	var insight map[string]any
	if err = decode([]byte(payload), &insight); err != nil {
		return documentEvidence{}, err
	}
	chunks := identity.DecodeJSON(excerpts)
	payloadHash, err := blueprints.PayloadHash([]byte(payload))
	if err != nil {
		return documentEvidence{}, err
	}
	entry := map[string]any{
		"evidence_ref":  "document:" + id,
		"document_id":   id,
		"display_name":  identity.Decode(name),
		"source_path":   identity.Decode(path),
		"source_origin": origin,
		"document_kind": kind,
		"insight": compactInsight(
			insight,
		),
		"excerpts":          chunks,
		"excerpts_partial":  true,
		"untrusted_content": true,
	}
	snapshot := map[string]any{
		"document_id":                 id,
		"source_hash":                 hash,
		"enrichment_source_hash":      analysisHash,
		"enrichment_analysis_version": version,
		"enrichment_model":            model,
		"enrichment_requested_model":  requested,
		"enrichment_context_hash":     contextHash,
		"enrichment_payload_hash":     payloadHash,
	}
	return documentEvidence{entry, snapshot}, nil
}

// collectExcerpts returns up to three representative chunk excerpts (first,
// middle, last by ordinal) for one document. Rows scan explicit columns and
// assemble the JSON-shaped maps in Go; the caller marshals each map with
// encoding/json only to measure the evidence budget.
func collectExcerpts(ctx context.Context, tx rdbms.Tx, document string) ([]any, error) {
	rows, err := tx.Query(ctx, `SELECT ordinal,locator_type,locator_start,left(text,1200) FROM knowledge.chunks WHERE document_id=$1 ORDER BY ordinal`, document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type excerpt struct {
		Ordinal int64
		Type    string
		Start   *string
		Text    string
	}
	all := []excerpt{}
	for rows.Next() {
		var item excerpt
		if err = rows.Scan(&item.Ordinal, &item.Type, &item.Start, &item.Text); err != nil {
			return nil, err
		}
		all = append(all, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return []any{}, nil
	}
	picks := []excerpt{all[0]}
	if len(all) > 2 {
		picks = append(picks, all[len(all)/2], all[len(all)-1])
	} else {
		picks = append(picks, all[1:]...)
	}
	result := make([]any, 0, len(picks))
	for _, item := range picks {
		result = append(result, map[string]any{
			"ordinal":       item.Ordinal,
			"locator_type":  item.Type,
			"locator_start": item.Start,
			"text":          item.Text,
		})
	}
	return result, nil
}
func clipped(s string, n int) string {
	r := []rune(strings.ReplaceAll(s, "\x00", ""))
	return string(r[:min(len(r), n)])
}
func compactInsight(in map[string]any) map[string]any {
	out := map[string]any{"summary": clipped(fmt.Sprint(in["summary"]), 2000), "details_truncated": true}
	for _, key := range []string{"material_type", "importance", "course_alignment", "assessment_relevance", "topics", "learning_objectives", "visual_content", "notable_items"} {
		switch v := in[key].(type) {
		case string:
			out[key] = clipped(v, 500)
		case []any:
			values := []any{}
			for _, item := range v[:min(5, len(v))] {
				if text, ok := item.(string); ok {
					values = append(values, clipped(text, 200))
				}
			}
			out[key] = values
		}
	}
	return out
}
