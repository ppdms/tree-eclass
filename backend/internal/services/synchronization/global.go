package synchronization

import (
	"context"
	"errors"
	"fmt"

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
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('global-feed:'||$1,0))`, key); err != nil {
		return err
	}
	lines := []string{}
	ids := []string{}
	for _, a := range items {
		if a.ID == "" {
			return errors.New("global announcement identity is required")
		}
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.global_announcements WHERE feed_key=$1 AND announcement_id=$2)`, key, identity.Encode(a.ID)).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			lines = append(lines, announcementLine(a))
			ids = append(ids, a.ID)
		}
		_, err = tx.Exec(
			ctx,
			`INSERT INTO app.global_announcements(feed_key,announcement_id,title,link,description,pub_date) VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(feed_key,announcement_id) DO UPDATE SET title=excluded.title,link=excluded.link,description=excluded.description,pub_date=excluded.pub_date,fetched_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
			key,
			identity.Encode(a.ID),
			identity.Encode(a.Title),
			a.Link,
			identity.Encode(a.Description),
			a.Published,
		)
		if err != nil {
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
