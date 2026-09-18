package materials

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
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

const materialPresentation = `SELECT d.id,d.course_id,d.display_name,d.source_path,d.source_origin,d.document_kind,d.status,d.diagnostic_reason,d.source_modified_at,d.indexed_at,d.source_size_bytes,d.page_count,d.reading_minutes,m.material_type,m.source_label,
CASE WHEN d.status='ready' AND e.status='ready' AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=$5
AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END AND pg_input_is_valid(e.payload_json,'jsonb')
THEN jsonb_build_object('external_material_type',e.payload_json::jsonb->>'external_material_type','material_type',e.payload_json::jsonb->>'material_type') ELSE '{}'::jsonb END
FROM knowledge.documents d LEFT JOIN app.external_material_metadata m ON m.course_id=d.course_id AND m.source_path=d.normalized_path
LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id
WHERE d.course_id=$1 AND d.is_current=1 AND d.source_origin='external' AND ($2='' OR d.id=$2) ORDER BY d.display_name,d.id`

func (s Service) List(ctx context.Context, id int64, document string, pendingAI bool) ([]Material, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`, id).Scan(&found); err != nil {
		return nil, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		ctx,
		materialPresentation,
		id,
		document,
		settings.DocumentAnalysisVersion,
		settings.PageSynthesisVersion,
		a.Model,
	)
	if err != nil {
		return nil, err
	}
	items := []Material{}
	for rows.Next() {
		item, err := readMaterial(rows, pendingAI)
		if err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return items, tx.Commit(ctx)
}

func readMaterial(row pgx.Row, pendingAI bool) (Material, error) {
	var item Material
	var override *string
	var raw []byte
	err := row.Scan(
		&item.ID,
		&item.CourseID,
		&item.Name,
		&item.Path,
		&item.Origin,
		&item.Kind,
		&item.Status,
		&item.Reason,
		&item.Modified,
		&item.Indexed,
		&item.Size,
		&item.Pages,
		&item.Minutes,
		&override,
		&item.Label,
		&raw,
	)
	if err != nil {
		return item, err
	}
	item.DocumentID = item.ID
	for _, value := range []*string{&item.Name, &item.Path, item.Label, item.Reason} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	manual := ""
	if override != nil {
		manual = *override
	}
	item.Type, item.Classification = classify(item.Path, manual)
	var payload map[string]string
	_ = json.Unmarshal(raw, &payload)
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
	return item, nil
}
