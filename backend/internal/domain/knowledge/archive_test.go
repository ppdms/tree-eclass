package knowledge

import (
	"strings"
	"testing"
	"tree-eclass/internal/integrations/parser"
)

func TestArchiveMemberIdentityAndBounds(t *testing.T) {
	valid := parser.Record{
		Type:           "member",
		MemberPath:     "nested.zip!/σημειώσεις.txt",
		MemberChain:    []string{"nested.zip", "σημειώσεις.txt"},
		Depth:          1,
		Kind:           "text",
		ArchiveFormat:  "zip",
		ContentHash:    strings.Repeat("a", 64),
		ExpandedSize:   20,
		CompressedSize: 18,
	}
	if err := validateMember(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*parser.Record){
		func(r *parser.Record) { r.MemberPath = "other.txt" },
		func(r *parser.Record) {
			r.MemberChain = []string{"../escape.txt"}
			r.MemberPath = "../escape.txt"
			r.Depth = 0
		},
		func(r *parser.Record) { r.ExpandedSize = 51 * 1024 * 1024 },
		func(r *parser.Record) {
			r.MemberChain = []string{"C:/escape.txt"}
			r.MemberPath = "C:/escape.txt"
			r.Depth = 0
		},
		func(r *parser.Record) { r.ContentHash = "unverified" },
		func(r *parser.Record) { r.Kind = "archive" },
		func(r *parser.Record) { r.ArchiveFormat = "unknown" },
	} {
		candidate := valid
		mutate(&candidate)
		if err := validateMember(candidate); err == nil {
			t.Fatal("invalid member admitted", candidate)
		}
	}
	path := archivePath("doc_test", "folder/a + b: c.txt")
	if path != "/.tree-eclass/archive-members/doc_test/folder%2Fa%20%2B%20b%3A%20c.txt" {
		t.Fatal("reserved path identity differs", path)
	}
	if archivePath("doc_test", "α\u0301.txt") != archivePath("doc_test", "ά.txt") {
		t.Fatal("archive path lost NFC identity")
	}
}
