package analysis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/integrations/parser"
)

func (s Service) excerpt(ctx context.Context, j job) (string, error) {
	tx, err := s.readSnapshot(ctx, j)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var rows []database.ExcerptRow
	maximum := 30000
	if j.Page > 0 {
		rows, err = tx.Analysis().PageExcerpts(ctx, j.Document.ID, fmt.Sprint(j.Page))
		maximum = 12000
	} else {
		rows, err = tx.Analysis().SampleExcerpts(ctx, j.Document.ID)
	}
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	remaining := maximum
	for _, row := range rows {
		value := "[" + row.LocatorType + " " + row.LocatorStart + "]\n" + identity.Decode(row.Text) + "\n\n"
		chars := []rune(value)
		chars = chars[:min(remaining, len(chars))]
		builder.WriteString(string(chars))
		remaining -= len(chars)
		if remaining == 0 {
			break
		}
	}
	return builder.String(), nil
}
func (s Service) pageImage(ctx context.Context, j job) (inference.Image, error) {
	ref, err := s.Pool.Analysis().PageImageObject(ctx, j.Document.ID, j.Document.Course, j.Document.Path, j.Document.Hash)
	if err != nil {
		return inference.Image{}, err
	}
	file, err := s.Objects.Download(ctx, blob.Reference{
		Bucket: ref.Bucket, Key: ref.Key, VersionID: ref.VersionID,
		SHA256: ref.SHA256, Bytes: ref.Bytes, MediaType: ref.MediaType,
	}, s.Temp)
	if err != nil {
		return inference.Image{}, err
	}
	defer os.Remove(file)
	result := inference.Image{MIMEType: "image/jpeg"}
	err = s.Parser.Run(
		ctx,
		parser.Request{
			Operation: "render",
			Path:      file,
			Kind:      j.Document.Kind,
			Pages:     []int{int(j.Page)},
			Options:   map[string]any{"max_dimension": 1600, "max_total_bytes": 2 * 1024 * 1024},
		},
		func(record parser.Record) error {
			if record.Type != "artifact" {
				return nil
			}
			if result.Data != "" || int64(record.Page) != j.Page {
				return errors.New("renderer returned an unexpected page")
			}
			f, err := os.Open(record.Path)
			if err != nil {
				return err
			}
			defer f.Close()
			raw, err := io.ReadAll(io.LimitReader(f, 2*1024*1024+1))
			if err != nil {
				return err
			}
			if len(raw) > 2*1024*1024 {
				return errors.New("rendered page exceeds 2 MiB")
			}
			result.Data = base64.StdEncoding.EncodeToString(raw)
			return nil
		},
	)
	if err == nil && result.Data == "" {
		err = errors.New("renderer returned no page image")
	}
	return result, err
}
func (s Service) pageEvidence(ctx context.Context, j job) (string, error) {
	tx, err := s.readSnapshot(ctx, j)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	perPage := (300000-2)/int(j.Document.Pages) - 1
	if perPage < 100 {
		return "", errors.New("complete page synthesis exceeds its 300000-character evidence limit")
	}
	rows, err := tx.Analysis().PageEvidence(ctx, j.Document.ID, j.Document.Hash,
		settings.PageAnalysisVersion, j.Requested, j.Document.Pages)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.WriteByte('[')
	count := int64(0)
	for _, row := range rows {
		if row.Payload == nil || row.PageNumber != count+1 {
			return "", errors.New("page analysis coverage is incomplete")
		}
		p, err := inference.ParseObject(*row.Payload)
		if err != nil {
			return "", err
		}
		compact, err := compactPage(p, row.PageNumber, perPage)
		if err != nil {
			return "", err
		}
		if count > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(compact)
		count++
	}
	if count != j.Document.Pages {
		return "", database.ErrNoRows
	}
	builder.WriteByte(']')
	return builder.String(), nil
}
func compactPage(p map[string]any, page int64, limit int) (string, error) {
	p["page_number"] = page
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	if utf8.RuneCount(raw) <= limit {
		return string(raw), nil
	}
	compact := map[string]any{
		"page":              page,
		"summary":           text(p["summary"], max(1, limit-100)),
		"page_type":         p["page_type"],
		"details_truncated": true,
	}
	for {
		raw, err = json.Marshal(compact)
		if err != nil {
			return "", err
		}
		if utf8.RuneCount(raw) <= limit {
			return string(raw), nil
		}
		summary := compact["summary"].(string)
		if utf8.RuneCountInString(summary) < 2 {
			return "", errors.New("page summary exceeds evidence budget")
		}
		compact["summary"] = text(summary, utf8.RuneCountInString(summary)/2)
	}
}
