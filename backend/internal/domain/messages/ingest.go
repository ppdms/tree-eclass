package messages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/infrastructure/rdbms"
)

// objectStore is the importer's object-storage contract: spool new archive
// bytes, or reopen a registered object when re-indexing.
type objectStore interface {
	Put(ctx context.Context, input io.Reader, mediaType, tempDir string) (objects.Reference, error)
	Open(ctx context.Context, ref objects.Reference) (io.ReadCloser, error)
}

type Importer struct {
	Pool  rdbms.Pool
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
func (s Importer) importTx(ctx context.Context, tx rdbms.Tx, source Archive, input io.Reader) (ImportResult, error) {
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

func (s Importer) publishImport(ctx context.Context, tx rdbms.Tx, source Archive, file *os.File) (ImportResult, error) {
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
	tx rdbms.Tx,
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

func stageArchive(ctx context.Context, tx rdbms.Tx, source Archive, input io.Reader) (exportHeader, int64, error) {
	if err := createStageTable(ctx, tx); err != nil {
		return exportHeader{}, 0, err
	}
	stream := newExportStream(input)
	var count int64
	var rows [][]any
	for {
		row, err := stageNext(source, stream, &count)
		if err == io.EOF || (err == nil && row == nil) {
			break
		}
		if err != nil {
			return exportHeader{}, 0, err
		}
		rows = append(rows, row)
	}
	copied, err := insertStageRows(ctx, tx, rows)
	if err != nil {
		return exportHeader{}, 0, err
	}
	return validateStage(source, stream, copied)
}

// insertStageRows loads staged messages with multi-row INSERTs so both
// drivers share one path: 500 rows per statement ($N placeholders per row).
func insertStageRows(ctx context.Context, tx rdbms.Tx, rows [][]any) (int64, error) {
	const chunk = 500
	cols := strings.Join(stageColumns, ",")
	var stmts []string
	var args [][]any
	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		var sb strings.Builder
		sb.WriteString(`INSERT INTO tree_discord_stage(`)
		sb.WriteString(cols)
		sb.WriteString(`) VALUES `)
		flat := make([]any, 0, len(batch)*len(stageColumns))
		for i, row := range batch {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(")
			for j := range stageColumns {
				if j > 0 {
					sb.WriteString(",")
				}
				fmt.Fprintf(&sb, "$%d", i*len(stageColumns)+j+1)
			}
			sb.WriteString(")")
			flat = append(flat, row...)
		}
		stmts = append(stmts, sb.String())
		args = append(args, flat)
	}
	if err := rdbms.Batch(ctx, tx, stmts, args); err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

func createStageTable(ctx context.Context, tx rdbms.Tx) error {
	_, err := tx.Exec(ctx, `DROP TABLE IF EXISTS tree_discord_stage`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`CREATE TEMP TABLE tree_discord_stage(message_id BIGINT PRIMARY KEY,timestamp TEXT NOT NULL,timestamp_epoch DOUBLE PRECISION NOT NULL,author_key TEXT,author_name TEXT NOT NULL,content TEXT NOT NULL,searchable_text TEXT NOT NULL,reply_to_message_id BIGINT,message_type TEXT NOT NULL,is_pinned BIGINT NOT NULL,reaction_count BIGINT NOT NULL,attachment_metadata_json TEXT NOT NULL)`,
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
