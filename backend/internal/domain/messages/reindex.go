package messages

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
)

// ReindexMapped repairs one archive after a channel is assigned to another
// course. Unmapped archives remain durable but are unavailable to readers.
func (s Importer) ReindexMapped(ctx context.Context) (bool, error) {
	path, source, object, err := s.loadMappedSource(ctx)
	if database.IsNoRows(err) {
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
	var object objects.Reference
	mapped, err := s.Pool.DiscordImports().MismappedSource(ctx)
	if err != nil {
		return "", source, object, err
	}
	source.Root = mapped.RootID
	source.Channel = mapped.ChannelID
	source.Course = mapped.CourseID
	return mapped.Path, source, fromObjectReference(mapped.Object), nil
}

func (s Importer) loadArchiveMedia(ctx context.Context, path string) (map[string]objects.Reference, error) {
	entries, err := s.Pool.DiscordImports().ArchiveMedia(ctx, path, 2001)
	if err != nil {
		return nil, err
	}
	media := make(map[string]objects.Reference, len(entries))
	for _, entry := range entries {
		media[identity.Decode(entry.RelativePath)] = fromObjectReference(entry.Object)
	}
	return media, nil
}
