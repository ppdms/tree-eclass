package materials

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type Material struct {
	ID             string  `json:"id"`
	DocumentID     string  `json:"document_id"`
	CourseID       int64   `json:"course_id"`
	Name           string  `json:"display_name"`
	Path           string  `json:"source_path"`
	Origin         string  `json:"source_origin"`
	Type           string  `json:"material_type"`
	Classification string  `json:"classification_source"`
	Label          *string `json:"source_label"`
	Kind           string  `json:"document_kind"`
	Status         string  `json:"status"`
	Reason         *string `json:"diagnostic_reason"`
	Modified       *string `json:"source_modified_at"`
	Indexed        *string `json:"indexed_at"`
	Size           *int64  `json:"source_size_bytes"`
	Pages          *int64  `json:"page_count"`
	Minutes        *int64  `json:"reading_minutes"`
	Renderable     bool    `json:"renderable"`
	OpenURL        *string `json:"open_url"`
	DownloadURL    *string `json:"download_url"`
}

func (s Service) List(ctx context.Context, id int64, document string, pendingAI bool) ([]Material, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Courses().VisibleCourse(ctx, id); err != nil {
		return nil, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Materials().ListPresentation(ctx, database.MaterialPresentationParams{
		CourseID:        id,
		DocumentID:      document,
		DocumentVersion: settings.DocumentAnalysisVersion,
		PageVersion:     settings.PageSynthesisVersion,
		Model:           a.Model,
	})
	if err != nil {
		return nil, err
	}
	items := make([]Material, 0, len(rows))
	for _, row := range rows {
		items = append(items, readMaterial(row, pendingAI))
	}
	return items, tx.Commit(ctx)
}

func readMaterial(row database.MaterialPresentationRow, pendingAI bool) Material {
	var item Material
	item.ID = row.ID
	item.CourseID = row.CourseID
	item.Name = row.DisplayName
	item.Path = row.SourcePath
	item.Origin = row.SourceOrigin
	item.Kind = row.DocumentKind
	item.Status = row.Status
	item.Reason = row.DiagnosticReason
	item.Modified = row.SourceModifiedAt
	item.Indexed = row.IndexedAt
	item.Size = row.SourceSizeBytes
	item.Pages = row.PageCount
	item.Minutes = row.ReadingMinutes
	item.Label = row.SourceLabel
	item.DocumentID = item.ID
	for _, value := range []*string{&item.Name, &item.Path, item.Label, item.Reason} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	manual := ""
	if row.MaterialType != nil {
		manual = *row.MaterialType
	}
	item.Type, item.Classification = classify(item.Path, manual)
	var payload map[string]string
	_ = json.Unmarshal(row.EnrichmentPayload, &payload)
	if manual == "" {
		if inferred := aiType(payload); inferred != "" {
			item.Type, item.Classification = inferred, "ai"
		} else if pendingAI {
			item.Classification = "pending"
		}
	}
	if item.Label == nil || *item.Label == "" {
		item.Label = sourceLabel(item.Path)
	}
	if item.Status == "ready" {
		download := fmt.Sprintf("/api/study/document/%s/content?course_id=%d", url.PathEscape(item.ID), item.CourseID)
		item.DownloadURL = &download
		if item.Kind == "pdf" {
			item.Renderable = true
			open := fmt.Sprintf("/study/session?course_id=%d&document_id=%s", item.CourseID, url.QueryEscape(item.ID))
			item.OpenURL = &open
		}
	}
	return item
}
