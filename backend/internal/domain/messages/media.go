package messages

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

func mapAttachments(m *stagedMessage, media map[string]blob.Reference) error {
	var attachments []map[string]any
	if err := decode([]byte(m.Attachments), &attachments); err != nil {
		return err
	}
	for _, a := range attachments {
		value, _ := a["url"].(string)
		value = identity.Decode(value)
		if value == "" {
			continue
		}
		parsed, err := url.Parse(value)
		if err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != "" &&
			parsed.User == nil {
			continue
		}
		object, exists := media[value]
		if !exists {
			return errors.New("Discord attachment references missing or unsafe media")
		}
		declared, ok := a["fileSizeBytes"].(json.Number)
		if !ok {
			return errors.New("Discord attachment is missing its declared byte count")
		}
		size, err := declared.Int64()
		if err != nil || size < 0 || size != object.Bytes {
			return errors.New("Discord attachment bytes differ from the completed export")
		}
		a["url"] = "/api/discord/media/" + object.SHA256
		a["object_id"] = object.SHA256
	}
	raw, err := json.Marshal(attachments)
	if err != nil {
		return err
	}
	m.Attachments = string(raw)
	return nil
}
func registerMedia(ctx context.Context, tx pgx.Tx, sourcePath string, media map[string]blob.Reference) error {
	for name, object := range media {
		if !strings.HasPrefix(name, "media/") || path.Clean(name) != name || strings.ContainsAny(name, "\x00\\") ||
			len(name) > 4096 ||
			object.Bytes > blob.MaxSourceBytes {
			return errors.New("invalid Discord media catalog reference")
		}
		if err := storage.RegisterObject(ctx, tx, object); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO messages.archive_media(source_path,relative_path,object_id) VALUES($1,$2,$3)`, sourcePath, identity.Encode(name), object.SHA256); err != nil {
			return err
		}
	}
	return nil
}
func (s Reader) Media(ctx context.Context, id string) (blob.Reference, error) {
	var object blob.Reference
	if len(id) != 64 || strings.ContainsAny(id, "/\\") {
		return object, pgx.ErrNoRows
	}
	err := s.Pool.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type FROM app.objects o WHERE o.id=$1 AND EXISTS(
 SELECT 1 FROM messages.archive_media am JOIN messages.archive_sources a ON a.path=am.source_path AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=a.course_id
 JOIN app.courses c ON c.id=a.course_id AND c.hidden=0 WHERE am.object_id=o.id)`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID, &object.SHA256, &object.Bytes, &object.MediaType)
	return object, err
}

func sameMedia(ctx context.Context, tx pgx.Tx, sourcePath string, media map[string]blob.Reference) error {
	rows, err := tx.Query(
		ctx,
		`SELECT relative_path,object_id FROM messages.archive_media WHERE source_path=$1 LIMIT 2001`,
		sourcePath,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, id string
		if err = rows.Scan(&name, &id); err != nil {
			return err
		}
		ref, exists := media[identity.Decode(name)]
		if !exists || ref.SHA256 != id {
			return errors.New("immutable Discord export has different attachment bytes")
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != len(media) {
		return errors.New("immutable Discord export has different attachment inventory")
	}
	return nil
}
