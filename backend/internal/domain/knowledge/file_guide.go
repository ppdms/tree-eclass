package knowledge

import (
	"context"

	"github.com/jackc/pgx/v5"
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
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT d.source_hash FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id WHERE d.id=$1 AND d.course_id=$2 AND d.is_current=1 AND d.status='ready' AND c.hidden=0`, document, course).
		Scan(&result.Hash)
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

func relatedMaterials(ctx context.Context, tx pgx.Tx, course int64, paths []string) ([]map[string]string, error) {
	result := []map[string]string{}
	rows, err := tx.Query(
		ctx,
		`SELECT source_path,display_name FROM knowledge.documents WHERE course_id=$1 AND normalized_path=ANY($2::text[]) AND is_current=1 AND status='ready' ORDER BY array_position($2::text[],normalized_path)`,
		course,
		paths,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var path, name string
		if err = rows.Scan(&path, &name); err != nil {
			return nil, err
		}
		result = append(result, map[string]string{"path": identity.Decode(path), "name": identity.Decode(name)})
	}
	return result, rows.Err()
}
