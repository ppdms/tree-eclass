package knowledge

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/extract"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
)

type archiveMember struct {
	Record         extract.Record
	Object         objects.Reference
	ID, Path, Name string
}

func archivePath(parent, member string) string {
	quote := func(s string) string { return strings.ReplaceAll(url.QueryEscape(norm.NFC.String(s)), "+", "%20") }
	return "/.tree-eclass/archive-members/" + quote(parent) + "/" + quote(member)
}
func memberArchive(d database.KnowledgeDocument) bool {
	ext := strings.ToLower(path.Ext(d.DisplayName))
	return d.DocumentKind == "archive" && (ext == ".zip" || ext == ".rar")
}

// ZIP/RAR content belongs to admitted leaves. Running the legacy flattening
// extractor first can mistake a ZIP nested inside a RAR for the outer archive.
func (i Indexer) archive(ctx context.Context, file string, d database.KnowledgeDocument, result *extraction) error {
	records := []extract.Record{}
	seen := map[string]bool{}
	var expanded int64
	err := i.Parser.Run(ctx, extract.Request{Operation: "archive-list", Path: file}, func(r extract.Record) error {
		if r.Type == "complete" {
			result.Warnings = append(result.Warnings, r.Warnings...)
			return nil
		}
		if r.Type != "member" {
			return nil
		}
		if err := validateMember(r); err != nil {
			return err
		}
		key := cases.Fold().String(norm.NFC.String(r.MemberPath))
		expanded += r.ExpandedSize
		if seen[key] || len(records) >= 1000 || expanded > 500*1024*1024 {
			return errors.New("archive exceeds member or expansion bounds")
		}
		seen[key] = true
		records = append(records, r)
		return nil
	})
	if err != nil {
		return err
	}
	for _, r := range records {
		member, err := i.archiveObject(ctx, file, d, r)
		if err != nil {
			return err
		}
		result.Members = append(result.Members, member)
	}
	return nil
}
func validateMember(r extract.Record) error {
	if r.Type != "member" || len(r.MemberChain) < 1 || len(r.MemberChain) > 2 || r.Depth != len(r.MemberChain)-1 ||
		r.MemberPath != strings.Join(r.MemberChain, "!/") {
		return errors.New("invalid archive member identity")
	}
	if !slices.Contains([]string{"pdf", "image", "text", "html", "source", "notebook"}, r.Kind) ||
		!slices.Contains([]string{"zip", "rar"}, r.ArchiveFormat) {
		return errors.New("unsupported archive member kind or container")
	}
	if len(r.ContentHash) != 64 || strings.Trim(r.ContentHash, "0123456789abcdef") != "" || r.ExpandedSize < 0 ||
		r.ExpandedSize > objects.MaxSourceBytes ||
		r.CompressedSize < 0 {
		return errors.New("invalid archive member size or hash")
	}
	for _, part := range r.MemberChain {
		if len(part) > 4096 || part == "" || strings.Contains(part, "\\") || strings.HasPrefix(part, "/") ||
			strings.ContainsFunc(part, unicode.IsControl) {
			return errors.New("unsafe archive member path")
		}
		components := strings.Split(part, "/")
		if len(components) > 32 {
			return errors.New("archive member path too deep")
		}
		for _, p := range components {
			if p == "" || p == "." || p == ".." || len(p) > 255 {
				return errors.New("unsafe archive member component")
			}
		}
		if len(components[0]) >= 2 && components[0][1] == ':' {
			return errors.New("drive-qualified archive member")
		}
	}
	return nil
}

func (i Indexer) archiveObject(
	ctx context.Context,
	file string,
	d database.KnowledgeDocument,
	r extract.Record,
) (archiveMember, error) {
	member := archiveMember{
		Record: r,
		Path:   archivePath(d.ID, r.MemberPath),
		Name:   path.Base(r.MemberChain[len(r.MemberChain)-1]),
	}
	member.ID = identity.Document(d.CourseID, member.Path)
	received := false
	request := extract.Request{
		Operation:     "archive-member",
		ArchiveFormat: r.ArchiveFormat,
		Path:          file,
		MemberChain:   r.MemberChain,
		Options: map[string]any{
			"expected_hash":            r.ContentHash,
			"expected_crc32":           r.CRC32,
			"expected_compressed_size": r.CompressedSize,
			"expected_expanded_size":   r.ExpandedSize,
		},
	}
	err := i.Parser.Run(ctx, request, func(artifact extract.Record) error {
		if artifact.Type != "artifact" {
			return nil
		}
		if received {
			return errors.New("archive returned duplicate member bytes")
		}
		received = true
		if artifact.Bytes != r.ExpandedSize {
			return errors.New("archive member size changed")
		}
		f, err := os.Open(artifact.Path)
		if err != nil {
			return err
		}
		defer f.Close()
		media := r.MIMEType
		if media == "" {
			media = "application/octet-stream"
		}
		member.Object, err = i.Objects.Put(ctx, f, media, i.Temp)
		if err != nil {
			return err
		}
		if member.Object.SHA256 != r.ContentHash || member.Object.Bytes != r.ExpandedSize {
			return errors.New("archive member content changed")
		}
		return nil
	})
	if err == nil && !received {
		err = errors.New("archive returned no member bytes")
	}
	return member, err
}
