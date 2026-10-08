package messages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/objects"
)

// objectStore is the importer's object-storage contract: spool new archive
// bytes, or reopen a registered object when re-indexing.
type objectStore interface {
	Put(ctx context.Context, input io.Reader, mediaType, tempDir string) (objects.Reference, error)
	Open(ctx context.Context, ref objects.Reference) (io.ReadCloser, error)
}

type Importer struct {
	Pool  database.Store
	Blobs objectStore
	Temp  string
}
type Archive struct {
	Media                 map[string]objects.Reference
	ExpectedSHA           string
	Root, Channel, Course int64
	After, Before         int64
}
type ImportResult struct {
	Path                    string
	Object                  objects.Reference
	Messages, Conversations int64
}

func fromObjectReference(ref database.ObjectReference) objects.Reference {
	return objects.Reference{
		Bucket:    ref.Bucket,
		Key:       ref.Key,
		VersionID: ref.VersionID,
		SHA256:    ref.SHA256,
		Bytes:     ref.Bytes,
		MediaType: ref.MediaType,
	}
}

func toStagedMessage(m stagedMessage) database.DiscordStagedMessage {
	return database.DiscordStagedMessage{
		ID:          m.ID,
		Timestamp:   m.Timestamp,
		Epoch:       m.Epoch,
		AuthorKey:   m.AuthorKey,
		AuthorName:  m.Author,
		Content:     m.Content,
		SearchText:  m.Search,
		ReplyTo:     m.Reply,
		MessageType: m.Type,
		Pinned:      m.Pinned,
		Reactions:   m.Reactions,
		Attachments: m.Attachments,
	}
}

func fromStagedMessage(m database.DiscordStagedMessage) stagedMessage {
	return stagedMessage{
		ID:          m.ID,
		Timestamp:   m.Timestamp,
		Epoch:       m.Epoch,
		AuthorKey:   m.AuthorKey,
		Author:      m.AuthorName,
		Content:     m.Content,
		Search:      m.SearchText,
		Reply:       m.ReplyTo,
		Type:        m.MessageType,
		Pinned:      m.Pinned,
		Reactions:   m.Reactions,
		Attachments: m.Attachments,
	}
}

// Import never trusts an exporter path or retains the parsed archive in memory.
// All derived rows and the verified object reference publish in one transaction.
func (s Importer) Import(ctx context.Context, source Archive, input io.Reader) (ImportResult, error) {
	var result ImportResult
	if source.Root <= 0 || source.Channel <= 0 || source.Course <= 0 || source.After < 0 || source.Before < 0 ||
		(source.Before > 0 && source.Before <= source.After) {
		return result, errors.New("invalid Discord archive scope")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = s.importTx(ctx, tx, source, input)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}
func (s Importer) importTx(ctx context.Context, tx database.Tx, source Archive, input io.Reader) (ImportResult, error) {
	var result ImportResult
	file, err := os.CreateTemp(s.Temp, "discord-import-*")
	if err != nil {
		return result, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	size, err := io.Copy(file, io.LimitReader(input, objects.MaxSourceBytes+1))
	if err != nil {
		return result, err
	}
	if size == 0 || size > objects.MaxSourceBytes {
		return result, errors.New("Discord export must contain 1 byte to 50 MiB")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	return s.publishImport(ctx, tx, source, file)
}

func (s Importer) publishImport(
	ctx context.Context, tx database.Tx, source Archive, file *os.File,
) (ImportResult, error) {
	var result ImportResult
	h, count, err := stageArchive(ctx, tx, source, file)
	if err != nil {
		return result, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	object, err := s.Blobs.Put(ctx, file, "application/json", s.Temp)
	if err != nil {
		return result, err
	}
	if source.ExpectedSHA != "" && source.ExpectedSHA != object.SHA256 {
		return result, errors.New("Discord archive bytes differ from immutable catalog")
	}
	result = ImportResult{
		Path:     fmt.Sprintf("discord/%d/%d/%s.json", source.Root, source.Channel, object.SHA256),
		Object:   object,
		Messages: count,
	}
	if err = lockArchive(ctx, tx, source, result.Path); err != nil {
		return result, err
	}
	current, err := tx.DiscordImports().ArchiveReady(ctx, database.DiscordArchiveIdentity{
		Path:     result.Path,
		CourseID: source.Course,
		SHA256:   object.SHA256,
	})
	if err != nil {
		return result, err
	}
	if current {
		if err = sameMedia(ctx, tx, result.Path, source.Media); err != nil {
			return result, err
		}
		result.Conversations, err = tx.DiscordImports().ArchiveConversationCount(ctx, result.Path)
		return result, err
	}
	return s.registerArchive(ctx, tx, source, h, result)
}

func (s Importer) registerArchive(
	ctx context.Context,
	tx database.Tx,
	source Archive,
	h exportHeader,
	result ImportResult,
) (ImportResult, error) {
	if err := objects.RegisterObject(ctx, tx, result.Object); err != nil {
		return result, err
	}
	if err := publishArchive(ctx, tx, source, h, result); err != nil {
		return result, err
	}
	if err := registerMedia(ctx, tx, result.Path, source.Media); err != nil {
		return result, err
	}
	var err error
	result.Conversations, err = buildConversations(ctx, tx, source, h, result.Path)
	return result, err
}

func stageArchive(
	ctx context.Context, tx database.Tx, source Archive, input io.Reader,
) (exportHeader, int64, error) {
	if err := tx.DiscordImports().BeginStage(ctx); err != nil {
		return exportHeader{}, 0, err
	}
	stream := newExportStream(input)
	var count int64
	var batch []database.DiscordStagedMessage
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := tx.DiscordImports().AppendStagedMessages(ctx, batch)
		batch = batch[:0]
		return err
	}
	for {
		row, err := stageNext(source, stream, &count)
		if err == io.EOF || (err == nil && row == nil) {
			break
		}
		if err != nil {
			return exportHeader{}, 0, err
		}
		batch = append(batch, *row)
		// Stream bounded batches so neither driver holds the whole export.
		if len(batch) >= 500 {
			if err = flush(); err != nil {
				return exportHeader{}, 0, err
			}
		}
	}
	if err := flush(); err != nil {
		return exportHeader{}, 0, err
	}
	return validateStage(source, stream, count)
}

func stageNext(
	source Archive, stream *exportStream, count *int64,
) (*database.DiscordStagedMessage, error) {
	raw, err := stream.next()
	if err == io.EOF {
		if !stream.done {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m, err := normalizeMessage(raw)
	if err != nil {
		return nil, err
	}
	if err = mapAttachments(&m, source.Media); err != nil {
		return nil, err
	}
	*count++
	if *count > 100000 {
		return nil, errors.New("Discord partition exceeds 100000 messages")
	}
	if m.ID <= source.After || (source.Before > 0 && m.ID >= source.Before) {
		return nil, errors.New("Discord message lies outside requested export interval")
	}
	staged := toStagedMessage(m)
	return &staged, nil
}

func validateStage(source Archive, stream *exportStream, copied int64) (exportHeader, int64, error) {
	h, err := stream.header()
	if err != nil {
		return h, 0, err
	}
	if int64(h.Channel.ID) != source.Channel {
		return h, 0, errors.New("Discord export channel does not match requested channel")
	}
	if raw, exists := stream.fields["messageCount"]; exists {
		var declared int64
		if err = json.Unmarshal(raw, &declared); err != nil || declared != copied {
			return h, 0, errors.New("Discord export message count differs from artifact")
		}
	}
	return h, copied, nil
}
