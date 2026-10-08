package synchronization

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/integrations/eclass"
)

func (s Service) crawl(ctx context.Context, source Source, root Directory, old Tree) (Tree, error) {
	tree := Tree{Directories: []Directory{root}, Files: []File{}}
	previous := map[string]File{}
	for _, f := range old.Files {
		previous[f.Parent+"\x00"+f.URL] = f
	}
	seen := map[string]bool{canonicalURL(root.URL): true}
	for i := 0; i < len(tree.Directories); i++ {
		d := tree.Directories[i]
		if len(tree.Directories) > 2000 || len(tree.Files) > 20000 || directoryDepth(d.Path, root.Path) > 32 {
			return tree, errors.New("course crawl exceeds directory/file/depth limits")
		}
		data, err := source.Page(ctx, d.URL)
		if err != nil {
			return tree, err
		}
		links, err := eclass.ParseLinks(data, d.URL)
		if err != nil {
			return tree, err
		}
		if len(tree.Files)+len(links.Files) > 20000 {
			return tree, errors.New("course crawl exceeds 20000 files")
		}
		for _, link := range links.Files {
			oldFile, exists := previous[d.Path+"\x00"+link.URL]
			file, err := s.fetch(ctx, source, d, link, oldFile, exists)
			if err != nil {
				return tree, fmt.Errorf("%s: %w", link.Name, err)
			}
			tree.Files = append(tree.Files, file)
		}
		for _, link := range links.Directories {
			key := canonicalURL(link.URL)
			if seen[key] {
				continue
			}
			if len(tree.Directories) >= 2000 || directoryDepth(d.Path, root.Path) >= 32 {
				return tree, errors.New("course crawl exceeds directory/depth limits")
			}
			seen[key] = true
			name, err := materials.Filename(link.Name)
			if err != nil {
				return tree, err
			}
			tree.Directories = append(
				tree.Directories,
				Directory{Parent: d.Path, Path: path.Join(d.Path, name), URL: link.URL, Name: name},
			)
		}
	}
	return tree, validateTree(tree)
}

func directoryDepth(value, root string) int {
	rel := relative(value, root)
	if rel == "" {
		return 0
	}
	return 1 + strings.Count(rel, "/")
}

func canonicalURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.RawQuery = u.Query().Encode()
	u.Fragment = ""
	return u.String()
}

func (s Service) fetch(
	ctx context.Context,
	source Source,
	dir Directory,
	link eclass.Link,
	old File,
	exists bool,
) (File, error) {
	f := File{Parent: dir.Path, URL: link.URL, Name: link.Name, Updated: time.Now().UTC().Format(time.RFC3339Nano)}
	var d eclass.Download
	var err error
	u, _ := url.Parse(link.URL)
	if u.Hostname() == "drive.google.com" {
		d, err = source.Drive(ctx, link.URL, link.Name)
	} else {
		etag := ""
		if old.Object != nil || old.Redirect != "" {
			etag = old.ETag
		}
		d, err = source.Download(ctx, link.URL, link.Name, etag)
	}
	if err != nil {
		return f, err
	}
	if d.Unchanged {
		if !exists || (old.Object == nil && old.Redirect == "") {
			return f, errors.New("upstream returned unchanged for a file without local content")
		}
		old.Name = link.Name
		return old, nil
	}
	if d.Body != nil {
		defer d.Body.Close()
	}
	actual, err := materials.Filename(d.Name)
	if err != nil {
		return f, err
	}
	f.Path, f.ETag, f.Redirect = identity.Path(path.Join(dir.Path, actual)), d.ETag, d.Redirect
	if f.Name == "" {
		f.Name = actual
	}
	if d.Redirect != "" {
		f.MD5 = fmt.Sprintf("%x", md5.Sum([]byte(d.Redirect)))
		return f, nil
	}
	if d.Body == nil {
		return f, errors.New("download returned no content")
	}
	hash := md5.New() // Legacy change-history fingerprint; object integrity uses SHA-256.
	object, err := s.Objects.Put(ctx, io.TeeReader(d.Body, hash), d.MediaType, s.Temp)
	if err != nil {
		return f, err
	}
	f.MD5, f.Object = fmt.Sprintf("%x", hash.Sum(nil)), &object
	if exists && old.MD5 == f.MD5 {
		f.Updated = old.Updated
	}
	return f, nil
}
