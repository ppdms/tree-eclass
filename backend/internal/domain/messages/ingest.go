package messages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

type Importer struct {
	Pool  *pgxpool.Pool
	Blobs *blob.Store
	Temp  string
}
type Archive struct {
	Media                 map[string]blob.Reference
	ExpectedSHA           string
	Root, Channel, Course int64
	After, Before         int64
}
type ImportResult struct {
	Path                    string
	Object                  blob.Reference
	Messages, Conversations int64
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
func (s Importer) importTx(ctx context.Context, tx pgx.Tx, source Archive, input io.Reader) (ImportResult, error) {
	var result ImportResult
	file, err := os.CreateTemp(s.Temp, "discord-import-*")
	if err != nil {
		return result, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	size, err := io.Copy(file, io.LimitReader(input, blob.MaxSourceBytes+1))
	if err != nil {
		return result, err
	}
	if size == 0 || size > blob.MaxSourceBytes {
		return result, errors.New("Discord export must contain 1 byte to 50 MiB")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	return s.publishImport(ctx, tx, source, file)
}

func (s Importer) publishImport(ctx context.Context, tx pgx.Tx, source Archive, file *os.File) (ImportResult, error) {
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
	var current bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messages.archive_sources WHERE path=$1 AND course_id=$2 AND fingerprint=$3 AND object_id=$3 AND status='ready')`, result.Path, source.Course, object.SHA256).Scan(&current); err != nil {
		return result, err
	}
	if current {
		if err = sameMedia(ctx, tx, result.Path, source.Media); err != nil {
			return result, err
		}
		err = tx.QueryRow(ctx, `SELECT count(*) FROM messages.conversations WHERE source_path=$1`, result.Path).
			Scan(&result.Conversations)
		return result, err
	}
	return s.registerArchive(ctx, tx, source, h, result)
}

func (s Importer) registerArchive(
	ctx context.Context,
	tx pgx.Tx,
	source Archive,
	h exportHeader,
	result ImportResult,
) (ImportResult, error) {
	if err := storage.RegisterObject(ctx, tx, result.Object); err != nil {
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

var stageColumns = []string{
	"message_id",
	"timestamp",
	"timestamp_epoch",
	"author_key",
	"author_name",
	"content",
	"searchable_text",
	"reply_to_message_id",
	"message_type",
	"is_pinned",
	"reaction_count",
	"attachment_metadata_json",
}

func stageArchive(ctx context.Context, tx pgx.Tx, source Archive, input io.Reader) (exportHeader, int64, error) {
	if err := createStageTable(ctx, tx); err != nil {
		return exportHeader{}, 0, err
	}
	stream := newExportStream(input)
	var count int64
	copied, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"tree_discord_stage"},
		stageColumns,
		pgx.CopyFromFunc(func() ([]any, error) {
			return stageNext(source, stream, &count)
		}),
	)
	if err != nil {
		return exportHeader{}, 0, err
	}
	return validateStage(source, stream, copied)
}

func createStageTable(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`DROP TABLE IF EXISTS pg_temp.tree_discord_stage; CREATE TEMP TABLE tree_discord_stage (LIKE messages.messages INCLUDING DEFAULTS) ON COMMIT DROP;
 ALTER TABLE tree_discord_stage ALTER COLUMN channel_id DROP NOT NULL,ALTER COLUMN course_id DROP NOT NULL,ALTER COLUMN source_path DROP NOT NULL;
 CREATE UNIQUE INDEX ON tree_discord_stage(message_id)`,
	)
	return err
}

func stageNext(source Archive, stream *exportStream, count *int64) ([]any, error) {
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
	return []any{
		m.ID,
		m.Timestamp,
		m.Epoch,
		m.AuthorKey,
		m.Author,
		m.Content,
		m.Search,
		m.Reply,
		m.Type,
		m.Pinned,
		m.Reactions,
		m.Attachments,
	}, nil
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
