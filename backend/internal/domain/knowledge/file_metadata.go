package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
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

// FileMetadata reads only compact columns; opening a file guide is a separate
// operation so browsing a large course never loads every analysis payload.
func (s Reader) FileMetadata(
	ctx context.Context,
	course int64,
	keys map[string]string,
) (map[string]FileMetadata, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = s.Visible(ctx, []int64{course}); err != nil {
		return nil, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	enabled := metadataAIEnabled(a, keys)
	rows, err := tx.Documents().FileMetadataRows(ctx, database.FileMetadataParams{
		Course: course, Model: a.Model, PageVersion: settings.PageAnalysisVersion,
		DocumentVersion: settings.DocumentAnalysisVersion, SynthesisVersion: settings.PageSynthesisVersion,
	})
	if err != nil {
		return nil, err
	}
	return scanFileMetadata(rows, course, enabled), tx.Commit(ctx)
}

func metadataAIEnabled(a settings.AI, keys map[string]string) bool {
	enabled := false
	for _, p := range settings.AnalysisProviders {
		enabled = enabled || (a.EnrichmentEnabled && a.Enabled(p) && settings.ProviderKey(keys, p) != "")
	}
	return enabled
}

func scanFileMetadata(rows []database.FileMetadataRow, course int64, enabled bool) map[string]FileMetadata {
	result := map[string]FileMetadata{}
	for _, row := range rows {
		item := FileMetadata{
			ID: row.ID, CourseID: course, Hash: row.Hash, Kind: row.Kind, Status: row.Status,
			Reason: row.Reason, Error: row.Error, Pages: row.Pages, Minutes: row.Minutes,
			Complexity: row.Complexity, Guide: row.Guide, AIEnabled: enabled,
			AnalysisStatus: row.Analysis, Model: row.Model, Version: row.Version,
			Generated: row.GeneratedAt, AnalysisError: row.AnalysisError,
			PagesReady: row.PagesReady, PagesTotal: row.PagesTotal, PagesEnabled: true,
		}
		item.Unit = unitName(item.Kind)
		for _, value := range []*string{item.Error, item.AnalysisError, item.Reason} {
			if value != nil {
				*value = identity.Decode(*value)
			}
		}
		result[identity.Decode(row.Path)] = item
	}
	return result
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
