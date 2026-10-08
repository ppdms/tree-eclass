package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/extract"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/domain/platform"
)

type Indexer struct {
	Pool    database.Store
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
	document, err := i.admitDocument(ctx, id)
	if err != nil {
		return err
	}
	defer func() {
		if failure != nil {
			failure = errors.Join(failure, i.recordFailure(ctx, id, document.SourceHash, failure))
		}
	}()
	row, err := i.Pool.Objects().DocumentObject(ctx, database.DocumentObjectParams{DocumentID: id})
	if err != nil {
		return err
	}
	object := objects.Reference{
		Bucket:    row.Object.Bucket,
		Key:       row.Object.Key,
		VersionID: row.Object.VersionID,
		SHA256:    row.Object.SHA256,
		Bytes:     row.Object.Bytes,
		MediaType: row.Object.MediaType,
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

func (i Indexer) admitDocument(
	ctx context.Context,
	id string,
) (database.KnowledgeDocument, error) {
	var document database.KnowledgeDocument
	free, err := platform.Available(i.Temp)
	if err != nil {
		return document, err
	}
	if free < 5*1024*1024*1024 {
		return document, errors.New("indexing paused: less than 5 GiB free disk space")
	}
	document, err = i.Pool.Indexing().IndexDocument(ctx, id)
	if err != nil {
		return document, err
	}
	admitted, err := i.Pool.Documents().DocumentAdmitted(ctx, id)
	if err != nil {
		return document, err
	}
	if !admitted {
		return document, ErrUnavailable
	}
	start := database.StartIndexRunParams{ID: id, Hash: document.SourceHash}
	if err = i.Pool.Indexing().StartIndexRun(ctx, start); err != nil {
		if database.IsNoRows(err) {
			return document, errors.New("document changed before extraction")
		}
		return document, err
	}
	return document, nil
}
func (i Indexer) download(ctx context.Context, ref objects.Reference) (string, error) {
	return i.Objects.Download(ctx, ref, i.Temp)
}
func (i Indexer) extract(ctx context.Context, file string, document database.KnowledgeDocument) (extraction, error) {
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
func (i Indexer) publish(ctx context.Context, document database.KnowledgeDocument, result extraction) error {
	tx, err := i.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Indexing().LockCourseForIndex(ctx, document.CourseID); err != nil {
		return err
	}
	if err = tx.Indexing().LockDocumentForIndex(ctx, document.ID); err != nil {
		return err
	}
	current, err := tx.Indexing().IndexDocument(ctx, document.ID)
	if err != nil {
		return err
	}
	admitted, err := tx.Documents().DocumentAdmitted(ctx, document.ID)
	if err != nil {
		return err
	}
	if !admitted || current.SourceHash != document.SourceHash {
		return errors.New("document changed while extracting; retry its new revision")
	}
	if err := publishChunks(ctx, tx, document, result); err != nil {
		return err
	}
	if err := markDocumentIndexed(ctx, tx, document.ID, result); err != nil {
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
	tx database.Tx,
	document database.KnowledgeDocument,
	result extraction,
) error {
	if err := tx.Indexing().ReplaceChunks(ctx, document.ID); err != nil {
		return err
	}
	for _, chunk := range result.Chunks {
		if err := insertChunk(ctx, tx, document, chunk); err != nil {
			return err
		}
	}
	return nil
}

func markDocumentIndexed(ctx context.Context, tx database.Tx, id string, result extraction) error {
	warnings, err := json.Marshal(result.Warnings)
	if err != nil {
		return err
	}
	metrics := result.Metrics.Result()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return tx.Indexing().MarkIndexed(
		ctx,
		database.MarkIndexedParams{
			ID:              id,
			PageCount:       &result.Pages,
			CharacterCount:  &metrics.Characters,
			WordCount:       &metrics.Words,
			ReadingMinutes:  &metrics.ReadingMinutes,
			ComplexityScore: &metrics.ComplexityScore,
			ComplexityLabel: &metrics.ComplexityLabel,
			IndexedAt:       &now,
			WarningsJSON:    string(warnings),
		},
	)
}
func insertChunk(ctx context.Context, tx database.Tx, doc database.KnowledgeDocument, c Chunk) error {
	metadata, err := json.Marshal(c.Metadata)
	if err != nil {
		return err
	}
	text, normalized := identity.Encode(c.Text), identity.Encode(c.NormalizedText)
	if err = tx.Indexing().InsertChunk(
		ctx,
		database.InsertChunkParams{
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
			MetadataJSON:   string(metadata),
		},
	); err != nil {
		return err
	}
	if err = tx.Indexing().IndexChunkSearch(
		ctx,
		database.IndexChunkSearchParams{
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
	return tx.Indexing().IndexEmbedding(
		ctx,
		database.IndexEmbeddingParams{
			ChunkID:    c.ID,
			Model:      LocalEmbeddingModel,
			Vector:     Pack(Embed(c.Text)),
			Dimensions: EmbeddingDimensions,
		},
	)
}
