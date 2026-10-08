package knowledge

import (
	"context"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

type FileMetadata struct {
	ID             string  `json:"id"`
	CourseID       int64   `json:"course_id"`
	Hash           string  `json:"source_hash"`
	Kind           string  `json:"document_kind"`
	Status         string  `json:"source_status"`
	Reason         *string `json:"source_diagnostic_reason"`
	Error          *string `json:"source_error"`
	Pages          *int64  `json:"page_count"`
	Unit           string  `json:"unit_name"`
	Minutes        *int64  `json:"reading_minutes"`
	Complexity     *string `json:"complexity_label"`
	Guide          bool    `json:"guide_available"`
	AIEnabled      bool    `json:"ai_processing_enabled"`
	AnalysisStatus string  `json:"enrichment_status"`
	Model          *string `json:"enrichment_model"`
	Version        *string `json:"enrichment_analysis_version"`
	Generated      *string `json:"enrichment_generated_at"`
	AnalysisError  *string `json:"enrichment_error"`
	PagesReady     int64   `json:"page_analysis_ready"`
	PagesTotal     int64   `json:"page_analysis_total"`
	PagesEnabled   bool    `json:"page_analysis_enabled"`
}

const fileMetadataQuery = `WITH target AS (
 SELECT d.* FROM knowledge.documents d WHERE d.course_id=$1 AND d.is_current=1
), pages AS (
 SELECT p.document_id,count(*) total,count(*) FILTER(WHERE p.status='ready') ready
 FROM knowledge.page_enrichments p JOIN target d ON d.id=p.document_id
 WHERE d.status='ready' AND p.source_hash=d.source_hash AND p.analysis_version=$3 AND p.requested_model=$2
 GROUP BY p.document_id
)
SELECT d.id,d.source_path,d.source_hash,d.document_kind,d.status,d.diagnostic_reason,left(d.error,300),d.page_count,d.reading_minutes,d.complexity_label,
 coalesce(e.status,'not_queued'),e.model,e.analysis_version,e.generated_at,CASE WHEN e.status='failed' THEN left(e.error,300) END,
 coalesce(e.status='ready' AND CASE WHEN pg_input_is_valid(e.payload_json,'jsonb') THEN jsonb_typeof(e.payload_json::jsonb->'summary')='string' AND length(trim(e.payload_json::jsonb->>'summary'))>0 ELSE false END,false),
 coalesce(p.ready,0),coalesce(p.total,0)
FROM target d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id AND d.status='ready' AND e.source_hash=d.source_hash AND coalesce(e.requested_model,e.model)=$2
 AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image') THEN $5 ELSE $4 END
LEFT JOIN pages p ON p.document_id=d.id ORDER BY d.source_path,d.id`

// FileMetadata reads only compact columns; opening a file guide is a separate
// operation so browsing a large course never loads every analysis payload.
func (s Reader) FileMetadata(
	ctx context.Context,
	course int64,
	keys map[string]string,
) (map[string]FileMetadata, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`, course).Scan(&id); err != nil {
		return nil, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	enabled := metadataAIEnabled(a, keys)
	rows, err := tx.Query(
		ctx,
		fileMetadataQuery,
		course,
		a.Model,
		settings.PageAnalysisVersion,
		settings.DocumentAnalysisVersion,
		settings.PageSynthesisVersion,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := scanFileMetadata(rows, course, enabled)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}

func metadataAIEnabled(a settings.AI, keys map[string]string) bool {
	enabled := false
	for _, p := range settings.AnalysisProviders {
		enabled = enabled || (a.EnrichmentEnabled && a.Enabled(p) && settings.ProviderKey(keys, p) != "")
	}
	return enabled
}

func scanFileMetadata(rows rdbms.Rows, course int64, enabled bool) (map[string]FileMetadata, error) {
	result := map[string]FileMetadata{}
	for rows.Next() {
		item := FileMetadata{CourseID: course, AIEnabled: enabled, PagesEnabled: true}
		var path string
		if err := rows.Scan(
			&item.ID,
			&path,
			&item.Hash,
			&item.Kind,
			&item.Status,
			&item.Reason,
			&item.Error,
			&item.Pages,
			&item.Minutes,
			&item.Complexity,
			&item.AnalysisStatus,
			&item.Model,
			&item.Version,
			&item.Generated,
			&item.AnalysisError,
			&item.Guide,
			&item.PagesReady,
			&item.PagesTotal,
		); err != nil {
			return nil, err
		}
		item.Unit = unitName(item.Kind)
		for _, value := range []*string{item.Error, item.AnalysisError, item.Reason} {
			if value != nil {
				*value = identity.Decode(*value)
			}
		}
		result[identity.Decode(path)] = item
	}
	return result, rows.Err()
}

func unitName(kind string) string {
	switch kind {
	case "pdf", "image":
		return "pages"
	case "presentation":
		return "slides"
	case "spreadsheet":
		return "sheets"
	case "notebook":
		return "cells"
	case "archive":
		return "files"
	case "source", "text", "html", "document":
		return "sections"
	default:
		return "units"
	}
}
