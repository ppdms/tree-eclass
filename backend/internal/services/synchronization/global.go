package synchronization

import (
	"context"
	"errors"
	"fmt"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
)

func (s Service) SaveGlobalAnnouncements(ctx context.Context, key string, items []eclass.Announcement) error {
	if key != "dept" && key != "undergrad" && key != "rector" {
		return errors.New("unknown global feed")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Sync().LockGlobalFeed(ctx, key); err != nil {
		return err
	}
	lines := []string{}
	ids := []string{}
	for _, a := range items {
		if a.ID == "" {
			return errors.New("global announcement identity is required")
		}
		exists, err := tx.Sync().GlobalAnnouncementExists(ctx, key, identity.Encode(a.ID))
		if err != nil {
			return err
		}
		if !exists {
			lines = append(lines, announcementLine(a))
			ids = append(ids, a.ID)
		}
		if err := tx.Sync().UpsertGlobalAnnouncement(ctx, database.SyncGlobalAnnouncementInput{
			FeedKey:     key,
			ID:          identity.Encode(a.ID),
			Title:       identity.Encode(a.Title),
			Link:        a.Link,
			Description: identity.Encode(a.Description),
			Published:   a.Published,
		}); err != nil {
			return err
		}
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	if len(lines) > 0 {
		err = notifications.EnqueueTx(
			ctx,
			tx,
			notifications.Event{
				Key:    identity.Stable("global-announcements", append([]string{key}, ids...)...),
				Header: "**New university announcements — " + key + "**",
				Lines:  lines,
			},
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s Service) globalFeeds(ctx context.Context, source *eclass.Client, prefs settings.Preferences) error {
	failures := []error{}
	for _, feed := range []struct {
		key     string
		enabled bool
	}{{"dept", prefs.DepartmentFeed}, {"undergrad", prefs.UndergradFeed}, {"rector", prefs.RectorFeed}} {
		if !feed.enabled {
			continue
		}
		items, err := source.GlobalAnnouncements(ctx, feed.key)
		if err == nil {
			err = s.SaveGlobalAnnouncements(ctx, feed.key, items)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("%s announcements: %w", feed.key, err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return errors.Join(failures...)
}
