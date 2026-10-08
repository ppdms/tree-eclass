package workspace

import (
	"context"
	"errors"

	"fmt"
	"net/url"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

func workspaceDocument(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	id string,
	a settings.AI,
) (map[string]any, error) {
	var name, path, origin, hash, kind string
	var mime, language *string
	var pages int64
	var minutes *int64
	err := tx.QueryRow(ctx, `SELECT display_name,source_path,source_origin,source_hash,document_kind,mime_type,coalesce(page_count,0),reading_minutes,language_hint
 FROM knowledge.documents d WHERE id=$1 AND course_id=$2 AND is_current=1 AND status='ready'
 AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.deleted_at IS NULL AND o.sha256=d.source_hash)`, id, course).
		Scan(&name, &path, &origin, &hash, &kind, &mime, &pages, &minutes, &language)
	if errors.Is(err, rdbms.ErrNoRows) && documentPending(ctx, tx, id, course) {
		return nil, ErrDocumentPending
	}
	if err != nil {
		return nil, err
	}
	var ready, marks, orphaned int64
	err = tx.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM knowledge.page_enrichments WHERE document_id=$1 AND source_hash=$2 AND analysis_version=$3 AND requested_model=$4 AND status='ready'),
 (SELECT count(*) FROM app.study_annotations WHERE document_id=$1 AND course_id=$5 AND status<>'deleted'),
 (SELECT count(*) FROM app.study_annotations WHERE document_id=$1 AND course_id=$5 AND status<>'deleted' AND (status='orphaned' OR source_hash<>$2))`, id, hash, settings.PageAnalysisVersion, a.Model, course).Scan(&ready, &marks, &orphaned)
	if err != nil {
		return nil, err
	}
	reading, err := readingTotals(ctx, tx, course, "", id)
	if err != nil {
		return nil, err
	}
	content := fmt.Sprintf("/api/study/document/%s/content?course_id=%d", url.PathEscape(id), course)
	return map[string]any{
		"document_id":          id,
		"course_id":            course,
		"display_name":         identity.Decode(name),
		"source_path":          identity.Decode(path),
		"source_origin":        origin,
		"source_hash":          hash,
		"document_kind":        kind,
		"mime_type":            mime,
		"page_count":           pages,
		"reading_minutes":      minutes,
		"language_hint":        language,
		"renderable":           kind == "pdf" || kind == "image",
		"content_url":          content,
		"download_url":         content,
		"insight_pages_ready":  ready,
		"reading":              reading,
		"annotation_count":     marks,
		"orphaned_annotations": orphaned,
	}, nil
}

// documentPending reports a registered current document whose content has not
// finished indexing yet, so the reader can wait instead of reporting it gone.
func documentPending(ctx context.Context, tx rdbms.Tx, id string, course int64) bool {
	var pending bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.documents
 WHERE id=$1 AND course_id=$2 AND is_current=1 AND status IN('pending','running'))`, id, course).Scan(&pending)
	return err == nil && pending
}

func actionDocuments(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	action map[string]any,
	a settings.AI,
) ([]map[string]any, error) {
	result := []map[string]any{}
	links, _ := action["evidence_links"].([]any)
	seen := map[string]bool{}
	for _, raw := range links {
		link, ok := raw.(map[string]any)
		if !ok || link["available"] != true {
			continue
		}
		id, _ := link["document_id"].(string)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		document, err := workspaceDocument(ctx, tx, course, id, a)
		if errors.Is(err, rdbms.ErrNoRows) || errors.Is(err, ErrDocumentPending) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, key := range []string{"evidence_ref", "evidence_class", "label"} {
			document[key] = link[key]
		}
		result = append(result, document)
	}
	return result, nil
}
