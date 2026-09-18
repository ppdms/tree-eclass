package courses

import (
	"testing"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func TestCatalogTreePreservesPathsAndRejectsDisconnectedNodes(t *testing.T) {
	rows := []queries.TreeNodesRow{
		{
			ID:        2,
			ParentID:  new(int64(1)),
			Name:      identity.Encode("Σημειώσεις\x00"),
			LocalPath: "/Courses/101/eclass/notes",
		},
		{ID: 1, Name: "Root", LocalPath: "/Courses/101/eclass"},
	}
	files := []queries.TreeFilesRow{{NodeID: 2, Name: "file.txt", LocalPath: new("/Courses/101/eclass/notes/file.txt")}}
	root, err := assembleTree(rows, files)
	if err != nil || root.Name != "Root" || len(root.Children) != 1 || root.Children[0].Name != "Σημειώσεις\x00" ||
		len(root.Children[0].Files) != 1 {
		t.Fatal("catalog tree", root, err)
	}
	if root.Files == nil || root.Children[0].Children == nil {
		t.Fatal("empty arrays became null")
	}
	rows = append(
		rows,
		queries.TreeNodesRow{ID: 3, ParentID: new(int64(4))},
		queries.TreeNodesRow{ID: 4, ParentID: new(int64(3))},
	)
	if _, err = assembleTree(rows, files); err == nil {
		t.Fatal("unreachable cycle silently dropped files")
	}
	rows[2].ParentID = nil
	if _, err = assembleTree(rows, files); err == nil {
		t.Fatal("multiple roots silently dropped a subtree")
	}
}
