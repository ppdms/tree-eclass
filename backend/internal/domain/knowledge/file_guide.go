package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type FileGuide struct {
	DocumentID string         `json:"document_id"`
	Hash       string         `json:"source_hash"`
	Status     string         `json:"status"`
	AI         map[string]any `json:"ai"`
}

func (s Reader) FileGuide(ctx context.Context, course int64, document string) (FileGuide, error) {
	result := FileGuide{DocumentID: document, Status: "not_queued"}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result.Hash, err = tx.Documents().FileGuideHash(ctx, course, document)
	if err != nil {
		return result, err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return result, err
	}
	analysis, err := readDocumentAnalysis(ctx, tx, a, document, result.Hash)
	if err != nil {
		return result, err
	}
	result.Status, _ = analysis["status"].(string)
	if analysis["ready"] == true {
		result.AI, _ = analysis["insight"].(map[string]any)
		paths := []string{}
		if raw, ok := result.AI["related_paths"].([]any); ok {
			for _, item := range raw {
				if path, ok := item.(string); ok && len(paths) < 100 {
					paths = append(paths, identity.Path(path))
				}
			}
		}
		result.AI["related_materials"], err = relatedMaterials(ctx, tx, course, paths)
		if err != nil {
			return result, err
		}
	}
	return result, tx.Commit(ctx)
}

func relatedMaterials(ctx context.Context, tx database.Tx, course int64, paths []string) ([]map[string]string, error) {
	result := []map[string]string{}
	rows, err := tx.Documents().RelatedMaterials(ctx, course, paths)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		result = append(result, map[string]string{"path": identity.Decode(row.Path), "name": identity.Decode(row.Name)})
	}
	return result, nil
}
