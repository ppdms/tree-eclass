package messages

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"path"
	"slices"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
)

func mapAttachments(m *stagedMessage, media map[string]objects.Reference) error {
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

func registerMedia(ctx context.Context, tx database.Tx, sourcePath string, media map[string]objects.Reference) error {
	// Deterministic order keeps same-tx registration backend-neutral.
	for _, name := range slices.Sorted(maps.Keys(media)) {
		object := media[name]
		if !strings.HasPrefix(name, "media/") || path.Clean(name) != name || strings.ContainsAny(name, "\x00\\") ||
			len(name) > 4096 ||
			object.Bytes > objects.MaxSourceBytes {
			return errors.New("invalid Discord media catalog reference")
		}
		if err := objects.RegisterObject(ctx, tx, object); err != nil {
			return err
		}
		if err := tx.DiscordImports().RegisterArchiveMedia(
			ctx, sourcePath, identity.Encode(name), object.SHA256,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s Reader) Media(ctx context.Context, id string) (objects.Reference, error) {
	var object objects.Reference
	if len(id) != 64 || strings.ContainsAny(id, "/\\") {
		return object, database.ErrNoRows
	}
	resolved, err := s.Pool.DiscordImports().ReadyMediaObject(ctx, id)
	if err != nil {
		return object, err
	}
	return fromObjectReference(resolved), nil
}

func sameMedia(ctx context.Context, tx database.Tx, sourcePath string, media map[string]objects.Reference) error {
	items, err := tx.DiscordImports().MediaInventory(ctx, sourcePath, 2001)
	if err != nil {
		return err
	}
	for _, item := range items {
		ref, exists := media[identity.Decode(item.RelativePath)]
		if !exists || ref.SHA256 != item.ObjectID {
			return errors.New("immutable Discord export has different attachment bytes")
		}
	}
	if len(items) != len(media) {
		return errors.New("immutable Discord export has different attachment inventory")
	}
	return nil
}
