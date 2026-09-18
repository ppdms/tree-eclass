package knowledge

import (
	"context"
	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

// Guide is bounded deterministic navigation, never an AI-generated policy source.
func (s Reader) Guide(ctx context.Context, id int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var visible bool
	if err = tx.QueryRow(ctx, `SELECT true FROM app.courses WHERE id=$1 AND hidden=0`, id).Scan(&visible); err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		ctx,
		`WITH selected AS (SELECT d.id,d.display_name,d.normalized_path FROM knowledge.documents d WHERE d.course_id=$1 AND `+CurrentSourcePredicate+` ORDER BY d.normalized_path,d.id LIMIT 100)
 SELECT 'material',display_name,normalized_path FROM selected UNION ALL SELECT 'heading',heading,heading FROM (SELECT DISTINCT k.heading FROM knowledge.chunks k JOIN selected s ON s.id=k.document_id WHERE k.heading IS NOT NULL AND k.heading<>'' ORDER BY k.heading LIMIT 100) h ORDER BY 1,3`,
		id,
	)
	if err != nil {
		return nil, err
	}
	materials, headings := []string{}, []string{}
	for rows.Next() {
		var kind, value, order string
		if err = rows.Scan(&kind, &value, &order); err != nil {
			rows.Close()
			return nil, err
		}
		if kind == "material" {
			materials = append(materials, identity.Decode(value))
		} else {
			headings = append(headings, identity.Decode(value))
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"materials":                materials,
		"headings":                 headings,
		"untrusted_content_notice": UntrustedNotice,
	}, tx.Commit(
		ctx,
	)
}
