package courses

import (
	"context"
	"slices"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
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

func (f *Files) history(ctx context.Context, tx rdbms.Tx, id int64) error {
	rows, err := tx.Query(
		ctx,
		`SELECT DISTINCT file_path,change_type FROM app.file_versions WHERE course_id=$1 AND change_type IN('modified','deleted') ORDER BY file_path`,
		id,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	deleted := map[string]bool{}
	for rows.Next() {
		var path, kind string
		if err = rows.Scan(&path, &kind); err != nil {
			return err
		}
		path = identity.Decode(path)
		if kind == "modified" {
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
	return rows.Err()
}

func (f *Files) preferences(ctx context.Context, tx rdbms.Tx, id int64) error {
	rows, err := tx.Query(
		ctx,
		`SELECT folder_key,collapsed FROM app.collapsed_course_folders WHERE course_id=$1 ORDER BY folder_key`,
		id,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		var collapsed int64
		if err = rows.Scan(&key, &collapsed); err != nil {
			rows.Close()
			return err
		}
		if collapsed != 0 {
			f.Collapsed = append(f.Collapsed, identity.Decode(key))
		} else {
			f.Expanded = append(f.Expanded, identity.Decode(key))
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT file_path,level FROM app.file_study WHERE course_id=$1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		var level int64
		if err = rows.Scan(&path, &level); err != nil {
			return err
		}
		f.Study[identity.Decode(path)] = level
	}
	return rows.Err()
}
