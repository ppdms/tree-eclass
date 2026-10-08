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
	"tree-eclass/internal/infrastructure/rdbms"
	"unicode/utf8"

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
	query := `WITH ranked AS(SELECT *,row_number() OVER(ORDER BY ordinal) n,count(*) OVER() total FROM knowledge.chunks WHERE document_id=$1)
 SELECT locator_type,coalesce(locator_start,''),left(text,2500) FROM ranked WHERE n IN(SELECT round(column1*(total-1)::numeric/11)+1 FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11))) ORDER BY ordinal`
	args := []any{j.Document.ID}
	maximum := 30000
	if j.Page > 0 {
		query = `SELECT locator_type,coalesce(locator_start,''),left(text,12000) FROM knowledge.chunks WHERE document_id=$1 AND locator_type='page' AND locator_start=$2 ORDER BY ordinal`
		args = append(args, fmt.Sprint(j.Page))
		maximum = 12000
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var builder strings.Builder
	remaining := maximum
	for rows.Next() {
		var kind, start, body string
		if err = rows.Scan(&kind, &start, &body); err != nil {
			return "", err
		}
		value := "[" + kind + " " + start + "]\n" + identity.Decode(body) + "\n\n"
		chars := []rune(value)
		chars = chars[:min(remaining, len(chars))]
		builder.WriteString(string(chars))
		remaining -= len(chars)
		if remaining == 0 {
			break
		}
	}
	return builder.String(), rows.Err()
}
func (s Service) pageImage(ctx context.Context, j job) (inference.Image, error) {
	var ref blob.Reference
	err := s.Pool.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id WHERE r.document_id=$1 AND r.course_id=$2 AND r.logical_path=$3 AND r.deleted_at IS NULL AND o.sha256=$4 ORDER BY r.created_at DESC LIMIT 1`, j.Document.ID, j.Document.Course, j.Document.Path, j.Document.Hash).
		Scan(&ref.Bucket, &ref.Key, &ref.VersionID, &ref.SHA256, &ref.Bytes, &ref.MediaType)
	if err != nil {
		return inference.Image{}, err
	}
	file, err := s.Objects.Download(ctx, ref, s.Temp)
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
	rows, err := tx.Query(
		ctx,
		`SELECT page_number,CASE WHEN octet_length(payload_json)<=262144 THEN payload_json END FROM knowledge.page_enrichments WHERE document_id=$1 AND source_hash=$2 AND analysis_version=$3 AND requested_model=$4 AND status='ready' AND page_number BETWEEN 1 AND $5 ORDER BY page_number`,
		j.Document.ID,
		j.Document.Hash,
		settings.PageAnalysisVersion,
		j.Requested,
		j.Document.Pages,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var builder strings.Builder
	builder.WriteByte('[')
	count := int64(0)
	for rows.Next() {
		var page int64
		var raw *string
		if err = rows.Scan(&page, &raw); err != nil {
			return "", err
		}
		if raw == nil || page != count+1 {
			return "", errors.New("page analysis coverage is incomplete")
		}
		p, err := inference.ParseObject(*raw)
		if err != nil {
			return "", err
		}
		compact, err := compactPage(p, page, perPage)
		if err != nil {
			return "", err
		}
		if count > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(compact)
		count++
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if count != j.Document.Pages {
		return "", rdbms.ErrNoRows
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
