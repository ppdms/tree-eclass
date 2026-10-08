package courses

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Node struct {
	Name     string  `json:"name"`
	URL      string  `json:"url"`
	Path     string  `json:"local_path"`
	Children []*Node `json:"children"`
	Files    []File  `json:"files"`
}
type File struct {
	URL      string  `json:"url"`
	Name     string  `json:"name"`
	MD5      *string `json:"md5_hash"`
	ETag     *string `json:"etag"`
	Updated  *string `json:"last_updated"`
	Path     *string `json:"local_path"`
	Redirect *string `json:"redirect_url"`
	Diff     *string `json:"diff_webdav_path"`
}

func (s Service) Tree(ctx context.Context, id int64) (*Node, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	root, err := treeSnapshot(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	return root, tx.Commit(ctx)
}

func treeSnapshot(ctx context.Context, tx rdbms.Tx, id int64) (*Node, error) {
	var found int64
	if err := tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0`, id).Scan(&found); err != nil {
		return nil, err
	}
	q := queries.ForTx(tx)
	nodes, err := q.TreeNodes(ctx, id)
	if err != nil {
		return nil, err
	}
	files, err := q.TreeFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	return assembleTree(nodes, files)
}

func assembleTree(rows []queries.TreeNodesRow, files []queries.TreeFilesRow) (*Node, error) {
	byID := map[int64]*Node{}
	var root *Node
	for _, row := range rows {
		node := &Node{
			Name:     identity.Decode(row.Name),
			URL:      row.Url,
			Path:     identity.Decode(row.LocalPath),
			Children: []*Node{},
			Files:    []File{},
		}
		byID[row.ID] = node
		if row.ParentID == nil {
			if root != nil {
				return nil, errors.New("course catalog contains multiple roots")
			}
			root = node
		}
	}
	for _, row := range rows {
		if row.ParentID == nil {
			continue
		}
		parent := byID[*row.ParentID]
		if parent == nil {
			return nil, errors.New("course directory belongs to an unavailable parent")
		}
		parent.Children = append(parent.Children, byID[row.ID])
	}
	if err := validateNodes(root, len(rows)); err != nil {
		return nil, err
	}
	if err := attachFiles(byID, files); err != nil {
		return nil, err
	}
	return root, nil
}

func attachFiles(byID map[int64]*Node, files []queries.TreeFilesRow) error {
	for _, row := range files {
		node := byID[row.NodeID]
		if node == nil {
			return errors.New("course file belongs to an unavailable directory")
		}
		file := File{
			URL:      row.Url,
			Name:     identity.Decode(row.Name),
			MD5:      row.Md5Hash,
			ETag:     row.Etag,
			Updated:  row.LastUpdated,
			Path:     row.LocalPath,
			Redirect: row.RedirectUrl,
		}
		if file.Path != nil {
			text := identity.Decode(*file.Path)
			file.Path = &text
		}
		node.Files = append(node.Files, file)
	}
	return nil
}

func validateNodes(root *Node, count int) error {
	if root == nil {
		if count == 0 {
			return nil
		}
		return errors.New("course catalog has no root")
	}
	type entry struct {
		Node  *Node
		Depth int
	}
	pending := []entry{{root, 0}}
	seen := map[*Node]bool{}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if item.Depth > 32 || seen[item.Node] {
			return errors.New("course catalog contains a cycle or exceeds 32 directory levels")
		}
		seen[item.Node] = true
		for _, child := range item.Node.Children {
			pending = append(pending, entry{child, item.Depth + 1})
		}
	}
	if len(seen) != count {
		return errors.New("course catalog contains unreachable directories")
	}
	return nil
}
