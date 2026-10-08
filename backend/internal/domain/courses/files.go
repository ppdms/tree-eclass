package courses

import (
	"context"
	"slices"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Files struct {
	Tree      *Node            `json:"tree"`
	Versions  []string         `json:"files_with_versions"`
	Deleted   []string         `json:"folders_with_deleted"`
	Collapsed []string         `json:"collapsed_folders"`
	Expanded  []string         `json:"expanded_folders"`
	Study     map[string]int64 `json:"study_levels"`
}

func (s Service) Files(ctx context.Context, id int64) (Files, error) {
	result := Files{
		Versions:  []string{},
		Deleted:   []string{},
		Collapsed: []string{},
		Expanded:  []string{},
		Study:     map[string]int64{},
	}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	if result.Tree, err = treeSnapshot(ctx, tx, id); err != nil {
		return result, err
	}
	if err = result.history(ctx, tx, id); err != nil {
		return result, err
	}
	if err = result.preferences(ctx, tx, id); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func (f *Files) history(ctx context.Context, tx database.Tx, id int64) error {
	changes, err := tx.Courses().FileVersionChanges(ctx, id)
	if err != nil {
		return err
	}
	deleted := map[string]bool{}
	for _, change := range changes {
		path := identity.Decode(change.FilePath)
		if change.ChangeType == "modified" {
			f.Versions = append(f.Versions, path)
			continue
		}
		parts := strings.Split(path, "/")
		for i := range len(parts) {
			deleted[strings.Join(parts[:i], "/")] = true
		}
	}
	for path := range deleted {
		f.Deleted = append(f.Deleted, path)
	}
	slices.Sort(f.Deleted)
	slices.Sort(f.Versions)
	return nil
}

func (f *Files) preferences(ctx context.Context, tx database.Tx, id int64) error {
	prefs, err := tx.Courses().FolderPreferences(ctx, id)
	if err != nil {
		return err
	}
	for _, pref := range prefs {
		if pref.Collapsed != 0 {
			f.Collapsed = append(f.Collapsed, identity.Decode(pref.FolderKey))
		} else {
			f.Expanded = append(f.Expanded, identity.Decode(pref.FolderKey))
		}
	}
	levels, err := tx.Courses().StudyLevelsForCourse(ctx, id)
	if err != nil {
		return err
	}
	for _, level := range levels {
		f.Study[identity.Decode(level.FilePath)] = level.Level
	}
	return nil
}
