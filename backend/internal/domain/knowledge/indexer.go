package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/extract"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/domain/queries"
)

type Indexer struct {
	Pool    *pgxpool.Pool
	Objects objects.Store
	Parser  extract.Extractor
	Temp    string
}
type extraction struct {
	Archive  bool
	Members  []archiveMember
	Chunks   []Chunk
	Pages    int64
	Metrics  metricAccumulator
	Warnings []string
}

func (i Indexer) Index(ctx context.Context, id string) (failure error) {
	free, err := platform.Available(i.Temp)
	if err != nil {
		return err
	}
	if free < 5*1024*1024*1024 {
		return errors.New("indexing paused: less than 5 GiB free disk space")
	}
	q := queries.New(i.Pool)
	document, err := q.IndexDocument(ctx, id)
	if err != nil {
		return err
	}
	var admitted bool
	if err = i.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.documents d WHERE d.id=$1 AND `+CurrentSourcePredicate+`)`, id).Scan(&admitted); err != nil {
		return err
	}
	if !admitted {
		return ErrUnavailable
	}
	changed, err := i.Pool.Exec(
		ctx,
		`UPDATE knowledge.documents SET status='running',error=NULL,diagnostic_reason=NULL WHERE id=$1 AND source_hash=$2 AND is_current=1`,
		id,
		document.SourceHash,
	)
	if err != nil {
		return err
	}
	if changed.RowsAffected() != 1 {
		return errors.New("document changed before extraction")
	}
	defer func() {
		if failure != nil {
			failure = errors.Join(failure, i.recordFailure(ctx, id, document.SourceHash, failure))
		}
	}()
	row, err := q.DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: id})
	if err != nil {
		return err
	}
	object := objects.Reference{
		Bucket:    row.Bucket,
		Key:       row.Key,
		VersionID: row.VersionID,
		SHA256:    row.Sha256,
		Bytes:     row.Bytes,
		MediaType: row.MediaType,
	}
	file, err := i.download(ctx, object)
	if err != nil {
		return err
	}
	defer os.Remove(file)
	result, err := i.extract(ctx, file, document)
	if err != nil {
		return err
	}
	return i.publish(ctx, document, result)
}
func (i Indexer) download(ctx context.Context, ref objects.Reference) (string, error) {
	return i.Objects.Download(ctx, ref, i.Temp)
}
func (i Indexer) extract(ctx context.Context, file string, document queries.KnowledgeDocument) (extraction, error) {
	result := extraction{Warnings: []string{}, Archive: document.DocumentKind == "archive"}
	if memberArchive(document) {
		err := i.archive(ctx, file, document, &result)
		return result, err
	}
	mediaType := ""
	if document.MimeType != nil {
		mediaType = *document.MimeType
	}
	source := &extract.Source{
		CourseID:        document.CourseID,
		CourseName:      identity.Decode(document.CourseName),
		CourseShortName: document.CourseShortName,
		SourcePath: identity.Decode(
			document.SourcePath,
		),
		SourceURL:   document.SourceUrl,
		DisplayName: identity.Decode(document.DisplayName),
		SourceHash:  document.SourceHash,
		MIMEType:    mediaType,
	}
	limits := map[string]any{"ocr_enabled": i.Parser.OCREnabled(), "ocr_languages": "ell+eng"}
	err := i.Parser.Run(
		ctx,
		extract.Request{Operation: "extract", Path: file, Kind: document.DocumentKind, Source: source, Limits: limits},
		func(record extract.Record) error {
			if record.Type == "complete" {
				result.Warnings = append(result.Warnings, record.Warnings...)
				return nil
			}
			if record.Type != "unit" {
				return nil
			}
			result.Metrics.Add(record.Text)
			if record.LocatorType == "page" {
				page, _ := strconv.ParseInt(record.LocatorStart, 10, 64)
				result.Pages = max(result.Pages, page)
			}
			result.Chunks = append(
				result.Chunks,
				Chunks(document.ID, document.SourceHash, record, int64(len(result.Chunks)))...)
			return nil
		},
	)
	return result, err
}
func (i Indexer) publish(ctx context.Context, document queries.KnowledgeDocument, result extraction) error {
	tx, err := i.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT id FROM app.courses WHERE id=$1 FOR UPDATE", document.CourseID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT id FROM knowledge.documents WHERE id=$1 FOR UPDATE", document.ID); err != nil {
		return err
	}
	q := queries.New(tx)
	current, err := q.IndexDocument(ctx, document.ID)
	if err != nil {
		return err
	}
	var admitted bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.documents d WHERE d.id=$1 AND `+CurrentSourcePredicate+`)`, document.ID).Scan(&admitted); err != nil {
		return err
	}
	if !admitted || current.SourceHash != document.SourceHash {
		return errors.New("document changed while extracting; retry its new revision")
	}
	if err := publishChunks(ctx, q, document, result); err != nil {
		return err
	}
	if err := markDocumentIndexed(ctx, q, document.ID, result); err != nil {
		return err
	}
	if result.Archive {
		if err = publishArchive(ctx, tx, current, result.Members); err != nil {
			return err
		}
	}
	if _, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func publishChunks(
	ctx context.Context,
	q *queries.Queries,
	document queries.KnowledgeDocument,
	result extraction,
) error {
	if err := q.ReplaceChunks(ctx, document.ID); err != nil {
		return err
	}
	for _, chunk := range result.Chunks {
		if err := insertChunk(ctx, q, document, chunk); err != nil {
			return err
		}
	}
	return nil
}

func markDocumentIndexed(ctx context.Context, q *queries.Queries, id string, result extraction) error {
	warnings, err := json.Marshal(result.Warnings)
	if err != nil {
		return err
	}
	metrics := result.Metrics.Result()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	err = q.MarkIndexed(
		ctx,
		queries.MarkIndexedParams{
			ID:              id,
			PageCount:       &result.Pages,
			CharacterCount:  &metrics.Characters,
			WordCount:       &metrics.Words,
			ReadingMinutes:  &metrics.ReadingMinutes,
			ComplexityScore: &metrics.ComplexityScore,
			ComplexityLabel: &metrics.ComplexityLabel,
			IndexedAt:       &now,
			WarningsJson:    string(warnings),
		},
	)
	return err
}
func insertChunk(ctx context.Context, q *queries.Queries, doc queries.KnowledgeDocument, c Chunk) error {
	metadata, err := json.Marshal(c.Metadata)
	if err != nil {
		return err
	}
	text, normalized := identity.Encode(c.Text), identity.Encode(c.NormalizedText)
	err = q.InsertChunk(
		ctx,
		queries.InsertChunkParams{
			ID:             c.ID,
			DocumentID:     c.DocumentID,
			Ordinal:        c.Ordinal,
			LocatorType:    c.LocatorType,
			LocatorStart:   &c.LocatorStart,
			LocatorEnd:     c.LocatorEnd,
			Heading:        c.Heading,
			Text:           text,
			NormalizedText: normalized,
			ContentHash:    c.ContentHash,
			MetadataJson:   string(metadata),
		},
	)
	if err != nil {
		return err
	}
	if err = q.IndexChunkSearch(
		ctx,
		queries.IndexChunkSearchParams{
			ChunkID:        c.ID,
			Text:           &text,
			NormalizedText: &normalized,
			Heading:        c.Heading,
			DisplayName:    &doc.DisplayName,
			SourcePath:     &doc.SourcePath,
			CourseName:     &doc.CourseName,
		},
	); err != nil {
		return err
	}
	return q.IndexEmbedding(
		ctx,
		queries.IndexEmbeddingParams{
			ChunkID:    c.ID,
			Model:      LocalEmbeddingModel,
			Vector:     Pack(Embed(c.Text)),
			Dimensions: EmbeddingDimensions,
		},
	)
}
