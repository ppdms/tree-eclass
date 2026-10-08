package messages

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/infrastructure/rdbms"
)

// ReindexMapped repairs one archive after a channel is assigned to another
// course. Unmapped archives remain durable but are unavailable to readers.
func (s Importer) ReindexMapped(ctx context.Context) (bool, error) {
	path, source, object, err := s.loadMappedSource(ctx)
	if errors.Is(err, rdbms.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	source.Media, err = s.loadArchiveMedia(ctx, path)
	if err != nil {
		return false, err
	}
	if len(source.Media) > 2000 {
		return false, errors.New("Discord media catalog exceeds limit")
	}
	source.ExpectedSHA = object.SHA256
	content, err := s.Blobs.Open(ctx, object)
	if err != nil {
		return false, err
	}
	defer content.Close()
	_, err = s.Import(ctx, source, content)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s Importer) loadMappedSource(ctx context.Context) (string, Archive, objects.Reference, error) {
	var source Archive
	var path string
	var object objects.Reference
	err := s.Pool.QueryRow(ctx, `SELECT a.path,a.root_id::bigint,a.channel_id,m.course_id,o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type
 FROM messages.archive_sources a JOIN app.discord_course_channels m ON m.root_channel_id=a.root_id JOIN app.objects o ON o.id=a.object_id
 WHERE a.course_id<>m.course_id ORDER BY a.indexed_at,a.path LIMIT 1`).
		Scan(
			&path,
			&source.Root,
			&source.Channel,
			&source.Course,
			&object.Bucket,
			&object.Key,
			&object.VersionID,
			&object.SHA256,
			&object.Bytes,
			&object.MediaType,
		)
	return path, source, object, err
}

func (s Importer) loadArchiveMedia(ctx context.Context, path string) (map[string]objects.Reference, error) {
	media := map[string]objects.Reference{}
	rows, err := s.Pool.Query(
		ctx,
		`SELECT am.relative_path,o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type FROM messages.archive_media am JOIN app.objects o ON o.id=am.object_id WHERE am.source_path=$1 LIMIT 2001`,
		path,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var ref objects.Reference
		if err = rows.Scan(
			&name,
			&ref.Bucket,
			&ref.Key,
			&ref.VersionID,
			&ref.SHA256,
			&ref.Bytes,
			&ref.MediaType,
		); err != nil {
			return nil, err
		}
		media[identity.Decode(name)] = ref
	}
	return media, rows.Err()
}
