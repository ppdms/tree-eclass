// Package synchronization publishes complete upstream observations atomically.
// Network failure never turns a previously downloaded file into a deletion.
package synchronization

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	Pool          *pgxpool.Pool
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
	return s.oldTreeFrom(ctx, s.Pool, id)
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (s Service) oldTreeFrom(ctx context.Context, db queryer, id int64) (Tree, error) {
	var tree Tree
	rows, err := db.Query(
		ctx,
		`SELECT n.local_path,coalesce(p.local_path,''),n.url,n.name FROM app.nodes n LEFT JOIN app.nodes p ON p.id=n.parent_id WHERE n.course_id=$1 ORDER BY n.id`,
		id,
	)
	if err != nil {
		return tree, err
	}
	for rows.Next() {
		var d Directory
		if err = rows.Scan(&d.Path, &d.Parent, &d.URL, &d.Name); err != nil {
			rows.Close()
			return tree, err
		}
		d.Path, d.Parent, d.Name = identity.Decode(d.Path), identity.Decode(d.Parent), identity.Decode(d.Name)
		tree.Directories = append(tree.Directories, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return tree, err
	}
	return s.oldFilesFrom(ctx, db, id, tree)
}

func (s Service) oldFilesFrom(ctx context.Context, db queryer, id int64, tree Tree) (Tree, error) {
	rows, err := db.Query(
		ctx,
		`SELECT n.local_path,f.url,f.name,coalesce(f.local_path,''),coalesce(f.md5_hash,''),coalesce(f.etag,''),coalesce(f.redirect_url,''),coalesce(f.last_updated,''),coalesce(f.revision_id,''),o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
FROM app.files f JOIN app.nodes n ON n.id=f.node_id LEFT JOIN app.objects o ON o.id=f.object_id WHERE n.course_id=$1 ORDER BY f.id`,
		id,
	)
	if err != nil {
		return tree, err
	}
	defer rows.Close()
	for rows.Next() {
		var f File
		var bucket, key, version, sha, media *string
		var size *int64
		if err = rows.Scan(
			&f.Parent,
			&f.URL,
			&f.Name,
			&f.Path,
			&f.MD5,
			&f.ETag,
			&f.Redirect,
			&f.Updated,
			&f.Revision,
			&bucket,
			&key,
			&version,
			&sha,
			&size,
			&media,
		); err != nil {
			return tree, err
		}
		f.Parent, f.Name, f.Path = identity.Decode(f.Parent), identity.Decode(f.Name), identity.Decode(f.Path)
		if bucket != nil {
			f.Object = &blob.Reference{
				Bucket:    *bucket,
				Key:       *key,
				VersionID: *version,
				SHA256:    *sha,
				Bytes:     *size,
				MediaType: *media,
			}
		}
		tree.Files = append(tree.Files, f)
	}
	return tree, rows.Err()
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
