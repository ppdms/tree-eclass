// Package extract defines the bounded document-extraction contract shared by
// domain services. It owns no process execution or storage.
package extract

import "context"

type Source struct {
	CourseID        int64   `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
	SourcePath      string  `json:"source_path"`
	SourceURL       *string `json:"source_url"`
	DisplayName     string  `json:"display_name"`
	SourceHash      string  `json:"source_hash"`
	MIMEType        string  `json:"mime_type"`
}
type Request struct {
	ArchiveFormat string         `json:"archive_format,omitempty"`
	Operation     string         `json:"operation"`
	Path          string         `json:"path"`
	Output        string         `json:"output"`
	Kind          string         `json:"kind,omitempty"`
	Source        *Source        `json:"source,omitempty"`
	Limits        map[string]any `json:"limits,omitempty"`
	Options       map[string]any `json:"options,omitempty"`
	Pages         []int          `json:"pages,omitempty"`
	MemberChain   []string       `json:"member_chain,omitempty"`
}
type Record struct {
	Type           string         `json:"type"`
	Title          string         `json:"title"`
	Kind           string         `json:"kind"`
	Text           string         `json:"text"`
	LocatorType    string         `json:"locator_type"`
	LocatorStart   string         `json:"locator_start"`
	LocatorEnd     *string        `json:"locator_end"`
	Heading        *string        `json:"heading"`
	Metadata       map[string]any `json:"metadata"`
	Warnings       []string       `json:"warnings"`
	Path           string         `json:"path"`
	Bytes          int64          `json:"bytes"`
	Page           int            `json:"page"`
	Reason         string         `json:"reason"`
	Message        string         `json:"message"`
	MemberPath     string         `json:"member_path"`
	MemberChain    []string       `json:"member_chain"`
	ContentHash    string         `json:"content_hash"`
	CRC32          uint32         `json:"crc32"`
	ExpandedSize   int64          `json:"expanded_size"`
	CompressedSize int64          `json:"compressed_size"`
	MIMEType       string         `json:"mime_type"`
	Depth          int            `json:"depth"`
	ArchiveFormat  string         `json:"archive_format"`
}

// Extractor runs bounded, short-lived extraction over source archives.
type Extractor interface {
	Run(ctx context.Context, req Request, consume func(Record) error) error
	OCREnabled() bool
}
