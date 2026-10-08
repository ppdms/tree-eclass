package synchronization

import (
	"context"
	"path"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/mirror"
)

func (s Service) MirrorExternal(ctx context.Context, id int64) error {
	prefs, err := (settings.Service{Pool: s.Pool}).Preferences(ctx)
	if err != nil {
		return err
	}
	root, err := resolveMirrorRoot(prefs.BasePath)
	if err != nil || root == "" {
		return err
	}
	course, err := s.Pool.Courses().Course(ctx, id)
	if err != nil {
		return err
	}
	s.MirrorRoot = root
	_, err = s.mirrorExternal(ctx, course)
	return err
}

func (s Service) mirrorExternal(ctx context.Context, course database.AppCourse) (mirror.Result, error) {
	files, err := s.externalMirrorFiles(ctx, course.ID)
	if err != nil {
		return mirror.Result{}, err
	}
	sourceRoot := path.Join(identity.Decode(course.WebdavFolder), "external")
	return (mirror.Service{Root: s.MirrorRoot, Objects: s.MirrorObjects}).Sync(
		ctx,
		mirror.Course{ID: course.ID, Name: identity.Decode(course.Name)},
		mirror.ExternalSubtree,
		sourceRoot,
		files,
	)
}

func (s Service) externalMirrorFiles(ctx context.Context, id int64) ([]mirror.File, error) {
	rows, err := s.Pool.Materials().ExternalMirrorFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	files := make([]mirror.File, 0, len(rows))
	for _, row := range rows {
		ref := blob.Reference{
			Bucket: row.Bucket, Key: row.Key, VersionID: row.VersionID,
			SHA256: row.SHA256, Bytes: row.Bytes,
		}
		files = append(files, mirror.File{Path: identity.Decode(row.NormalizedPath), Object: &ref})
	}
	return files, nil
}
