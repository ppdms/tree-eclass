package synthesis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type documentEvidence struct{ Entry, Snapshot map[string]any }

func collectDocuments(
	ctx context.Context,
	tx database.Tx,
	course int64,
	a settings.AI,
) ([]documentEvidence, int64, int64, error) {
	params := database.SynthesisEvidenceParams{
		CourseID: course, Model: a.Model,
		DocVersion: settings.DocumentAnalysisVersion, PageVersion: settings.PageSynthesisVersion,
		Limit: 100,
	}
	counts, err := tx.Synthesis().EvidenceCounts(ctx, params)
	if err != nil {
		return nil, 0, 0, err
	}
	total, ready := counts.Total, counts.Ready
	threshold := min(total, max(int64(3), min(int64(8), (total+19)/20)))
	if ready == 0 || ready < threshold {
		return nil, total, ready, nil
	}
	docs, err := tx.Synthesis().EvidenceDocuments(ctx, params)
	if err != nil {
		return nil, total, ready, err
	}
	result := []documentEvidence{}
	budget := 0
	for _, doc := range docs {
		item, err := readDocument(ctx, tx, doc)
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
	return result, total, ready, nil
}

// readDocument renders one ranked document row, then collects up to three
// representative chunk excerpts (first, middle, last).
func readDocument(ctx context.Context, tx database.Tx, doc database.SynthesisDocument) (documentEvidence, error) {
	excerpts, err := collectExcerpts(ctx, tx, doc.ID)
	if err != nil {
		return documentEvidence{}, err
	}
	var insight map[string]any
	if err = decode([]byte(doc.Payload), &insight); err != nil {
		return documentEvidence{}, err
	}
	chunks := identity.DecodeJSON(excerpts)
	payloadHash, err := blueprints.PayloadHash([]byte(doc.Payload))
	if err != nil {
		return documentEvidence{}, err
	}
	entry := map[string]any{
		"evidence_ref":  "document:" + doc.ID,
		"document_id":   doc.ID,
		"display_name":  identity.Decode(doc.DisplayName),
		"source_path":   identity.Decode(doc.SourcePath),
		"source_origin": doc.SourceOrigin,
		"document_kind": doc.DocumentKind,
		"insight": compactInsight(
			insight,
		),
		"excerpts":          chunks,
		"excerpts_partial":  true,
		"untrusted_content": true,
	}
	snapshot := map[string]any{
		"document_id":                 doc.ID,
		"source_hash":                 doc.SourceHash,
		"enrichment_source_hash":      doc.AnalysisHash,
		"enrichment_analysis_version": doc.Version,
		"enrichment_model":            doc.Model,
		"enrichment_requested_model":  doc.Requested,
		"enrichment_context_hash":     doc.ContextHash,
		"enrichment_payload_hash":     payloadHash,
	}
	return documentEvidence{entry, snapshot}, nil
}

// collectExcerpts returns up to three representative chunk excerpts (first,
// middle, last by ordinal) for one document.
func collectExcerpts(ctx context.Context, tx database.Tx, document string) ([]any, error) {
	all, err := tx.Synthesis().DocumentExcerpts(ctx, document)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return []any{}, nil
	}
	picks := []database.SynthesisExcerpt{all[0]}
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
