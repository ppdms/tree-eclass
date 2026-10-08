package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// Guide is bounded deterministic navigation, never an AI-generated policy source.
func (s Reader) Guide(ctx context.Context, id int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = s.Visible(ctx, []int64{id}); err != nil {
		return nil, err
	}
	encodedMaterials, encodedHeadings, err := tx.Documents().GuideNavigation(ctx, id)
	if err != nil {
		return nil, err
	}
	materials, headings := []string{}, []string{}
	for _, value := range encodedMaterials {
		materials = append(materials, identity.Decode(value))
	}
	for _, value := range encodedHeadings {
		headings = append(headings, identity.Decode(value))
	}
	return map[string]any{
		"materials":                materials,
		"headings":                 headings,
		"untrusted_content_notice": UntrustedNotice,
	}, tx.Commit(ctx)
}
