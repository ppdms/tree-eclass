// Package synchronization publishes complete upstream observations atomically.
// Network failure never turns a previously downloaded file into a deletion.
package synchronization

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/eclass"
)

type Source interface {
	Page(context.Context, string) ([]byte, error)
	Download(context.Context, string, string, string) (eclass.Download, error)
	Drive(context.Context, string, string) (eclass.Download, error)
}
type Objects interface {
	Put(context.Context, io.Reader, string, string) (blob.Reference, error)
}
type Service struct {
	Pool          database.Store
	Objects       Objects
	Temp          string
	MirrorRoot    string
	MirrorObjects *blob.Store
}
type File struct {
	Parent, URL, Name, Path, MD5, ETag, Redirect, Updated, Revision string
	Object                                                          *blob.Reference
}
type Directory struct{ Parent, Path, URL, Name string }
type Tree struct {
	Directories []Directory
	Files       []File
}
type Change struct {
	Current                    *File
	Type, Path, Name, Redirect string
	Previous                   *File
}
type Result struct {
	Added, Modified, Deleted int
	FilesAdded, FilesChanged int
	Changes                  []Change
}

func (s Service) oldTree(ctx context.Context, id int64) (Tree, error) {
	dirs, err := s.Pool.Sync().ListTreeDirectories(ctx, id)
	if err != nil {
		return Tree{}, err
	}
	paths := make(map[int64]string, len(dirs))
	for _, d := range dirs {
		paths[d.ID] = d.Path
	}
	var tree Tree
	for _, d := range dirs {
		parent := ""
		if d.ParentID != nil {
			parent = paths[*d.ParentID]
		}
		tree.Directories = append(tree.Directories, Directory{
			Parent: identity.Decode(parent),
			Path:   identity.Decode(d.Path),
			URL:    d.URL,
			Name:   identity.Decode(d.Name),
		})
	}
	return s.oldFiles(ctx, id, tree)
}

func (s Service) oldFiles(ctx context.Context, id int64, tree Tree) (Tree, error) {
	rows, err := s.Pool.Sync().ListTreeFiles(ctx, id)
	if err != nil {
		return tree, err
	}
	for _, row := range rows {
		f := File{
			Parent:   identity.Decode(row.Parent),
			URL:      row.URL,
			Name:     identity.Decode(row.Name),
			Path:     identity.Decode(row.Path),
			MD5:      row.MD5,
			ETag:     row.ETag,
			Redirect: row.Redirect,
			Updated:  row.Updated,
			Revision: row.Revision,
		}
		if row.Object != nil {
			f.Object = &blob.Reference{
				Bucket:    row.Object.Bucket,
				Key:       row.Object.Key,
				VersionID: row.Object.VersionID,
				SHA256:    row.Object.SHA256,
				Bytes:     row.Object.Bytes,
				MediaType: row.Object.MediaType,
			}
		}
		tree.Files = append(tree.Files, f)
	}
	return tree, nil
}

func validateTree(tree Tree) error {
	paths := map[string]bool{}
	for _, d := range tree.Directories {
		if paths[d.Path] {
			return errors.New("duplicate directory destination in course crawl")
		}
		paths[d.Path] = true
	}
	for _, f := range tree.Files {
		if f.Path == "" || paths[f.Path] {
			return fmt.Errorf("duplicate or empty file destination: %s", f.Name)
		}
		paths[f.Path] = true
	}
	return nil
}

func relative(raw, root string) string { return strings.TrimPrefix(strings.TrimPrefix(raw, root), "/") }
