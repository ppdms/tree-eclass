package materials

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/queries"
)

func (s Service) UpdateType(ctx context.Context, id int64, document, kind string) error {
	if _, ok := TypeFolders[kind]; !ok {
		return errors.New("choose a valid document type")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0 FOR SHARE`, id).Scan(&found); err != nil {
		return err
	}
	q := queries.ForTx(tx)
	row, err := q.Material(ctx, queries.MaterialParams{CourseID: id, ID: document})
	if err != nil {
		return err
	}
	if err = q.MaterialMetadata(
		ctx,
		queries.MaterialMetadataParams{
			CourseID:     id,
			SourcePath:   row.NormalizedPath,
			MaterialType: kind,
		},
	); err != nil {
		return err
	}
	if _, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
