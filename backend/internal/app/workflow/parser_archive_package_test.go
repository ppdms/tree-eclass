package workflow

import (
	"fmt"
	"os"
	"testing"

	"tree-eclass/internal/integrations/parser"
)

func packagedArchiveChecks(t *testing.T, runner *parser.Runner, file, want string) {
	t.Helper()
	var members []parser.Record
	err := runner.Run(t.Context(), parser.Request{Operation: "archive-list", Path: file}, func(r parser.Record) error {
		if r.Type == "member" {
			members = append(members, r)
		}
		return nil
	})
	if err != nil || len(members) != 1 {
		t.Fatal("packaged archive listing", members, err)
	}
	m := members[0]
	received := false
	request := parser.Request{
		Operation:     "archive-member",
		Path:          file,
		ArchiveFormat: m.ArchiveFormat,
		MemberChain:   m.MemberChain,
		Options: map[string]any{
			"expected_hash":            m.ContentHash,
			"expected_crc32":           m.CRC32,
			"expected_compressed_size": m.CompressedSize,
			"expected_expanded_size":   m.ExpandedSize,
		},
	}
	err = runner.Run(t.Context(), request, func(r parser.Record) error {
		if r.Type != "artifact" {
			return nil
		}
		data, err := os.ReadFile(r.Path)
		if err != nil {
			return err
		}
		if received || string(data) != want || r.Bytes != int64(len(data)) {
			return fmt.Errorf("unexpected packaged archive member: %q", data)
		}
		received = true
		return nil
	})
	if err != nil || !received {
		t.Fatal("packaged archive member", received, err)
	}
}
