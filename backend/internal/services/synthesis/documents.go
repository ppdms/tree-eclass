package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
)

const sourceFrom = ` FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id
 WHERE d.course_id=$1 AND d.document_kind<>'archive' AND d.status NOT IN('unsupported','skipped') AND ` + knowledge.CurrentSourcePredicate
const readySource = `d.status='ready' AND d.content_hash_verified=1 AND e.status='ready' AND e.source_hash=d.source_hash
 AND coalesce(e.requested_model,e.model)=$2 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END
 AND e.payload_json IS JSON OBJECT AND octet_length(e.payload_json)<=1048576
 AND coalesce((CASE WHEN e.payload_json IS JSON OBJECT THEN e.payload_json::jsonb ELSE '{}'::jsonb END)->>'summary','')<>'' AND coalesce((CASE WHEN e.payload_json IS JSON OBJECT THEN e.payload_json::jsonb ELSE '{}'::jsonb END)->>'course_alignment','')<>'mismatch'`

type documentEvidence struct{ Entry, Snapshot map[string]any }

func collectDocuments(
	ctx context.Context,
	tx pgx.Tx,
	course int64,
	a settings.AI,
) ([]documentEvidence, int64, int64, error) {
	args := []any{course, a.Model, settings.DocumentAnalysisVersion, settings.PageSynthesisVersion}
	var total, ready int64
	err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE `+readySource+`)`+sourceFrom, args...).
		Scan(&total, &ready)
	if err != nil {
		return nil, 0, 0, err
	}
	threshold := min(total, max(int64(3), min(int64(8), (total+19)/20)))
	if ready == 0 || ready < threshold {
		return nil, total, ready, nil
	}
	rows, err := tx.Query(ctx, `SELECT d.id,d.source_hash,d.display_name,d.source_path,d.source_origin,d.document_kind,
 e.source_hash,e.analysis_version,e.model,coalesce(e.requested_model,e.model),e.context_hash,e.payload_json,
 coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.ordinal) FROM(
 SELECT ordinal,locator_type,locator_start,left(text,1200) text FROM (
 SELECT *,row_number() OVER(ORDER BY ordinal) n,count(*) OVER() total FROM knowledge.chunks WHERE document_id=d.id) chunks
 WHERE n IN(1,greatest(1,total/2),total) ORDER BY ordinal LIMIT 3) x),'[]')`+sourceFrom+` AND `+readySource+`
 ORDER BY CASE (CASE WHEN e.payload_json IS JSON OBJECT THEN e.payload_json::jsonb ELSE '{}'::jsonb END)->>'importance' WHEN 'essential' THEN 0 WHEN 'useful' THEN 1 ELSE 2 END,d.id LIMIT 100`, args...)
	if err != nil {
		return nil, total, ready, err
	}
	defer rows.Close()
	result := []documentEvidence{}
	budget := 0
	for rows.Next() {
		item, err := readDocument(rows)
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
func readDocument(row pgx.Row) (documentEvidence, error) {
	var id, hash, name, path, origin, kind, analysisHash, version, model, requested, contextHash, payload string
	var excerpts []byte
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
		&excerpts,
	)
	if err != nil {
		return documentEvidence{}, err
	}
	var insight map[string]any
	var chunks []any
	if err = decode([]byte(payload), &insight); err != nil {
		return documentEvidence{}, err
	}
	if err = decode(excerpts, &chunks); err != nil {
		return documentEvidence{}, err
	}
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
		"excerpts":          identity.DecodeJSON(chunks),
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
