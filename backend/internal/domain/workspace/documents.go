package workspace

import (
	"context"
	"errors"

	"fmt"
	"net/url"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func workspaceDocument(
	ctx context.Context,
	tx database.Operations,
	course int64,
	id string,
	a settings.AI,
) (map[string]any, error) {
	doc, err := tx.Workspace().WorkspaceDocument(ctx, course, id)
	if errors.Is(err, database.ErrNoRows) {
		pending, pendingErr := tx.Workspace().DocumentPending(ctx, course, id)
		if pendingErr == nil && pending {
			return nil, ErrDocumentPending
		}
		if pendingErr != nil {
			return nil, pendingErr
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	counts, err := tx.Workspace().DocumentInsights(ctx, database.DocumentInsightParams{
		Document:        id,
		SourceHash:      doc.SourceHash,
		AnalysisVersion: settings.PageAnalysisVersion,
		RequestedModel:  a.Model,
		CourseID:        course,
	})
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
		"display_name":         identity.Decode(doc.DisplayName),
		"source_path":          identity.Decode(doc.SourcePath),
		"source_origin":        doc.SourceOrigin,
		"source_hash":          doc.SourceHash,
		"document_kind":        doc.DocumentKind,
		"mime_type":            doc.MimeType,
		"page_count":           doc.PageCount,
		"reading_minutes":      doc.ReadingMinutes,
		"language_hint":        doc.LanguageHint,
		"renderable":           doc.DocumentKind == "pdf" || doc.DocumentKind == "image",
		"content_url":          content,
		"download_url":         content,
		"insight_pages_ready":  counts.ReadyPages,
		"reading":              reading,
		"annotation_count":     counts.Annotations,
		"orphaned_annotations": counts.Orphaned,
	}, nil
}

func actionDocuments(
	ctx context.Context,
	tx database.Operations,
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
		if errors.Is(err, database.ErrNoRows) || errors.Is(err, ErrDocumentPending) {
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
