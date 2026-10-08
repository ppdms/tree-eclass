package materials

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
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
	if _, err = tx.Courses().LockCourseForWrite(ctx, id); err != nil {
		return err
	}
	row, err := tx.Materials().GetMaterial(ctx, id, document)
	if err != nil {
		return err
	}
	if err = tx.Materials().SetMaterialMetadata(
		ctx,
		database.MaterialMetadataParams{
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
