package settings

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Discord struct {
	Enabled  bool   `json:"enabled"`
	Token    string `json:"-"`
	Interval int64  `json:"interval_seconds"`
	Threads  string `json:"include_threads"`
	Media    bool   `json:"media"`
	Parallel int64  `json:"parallel"`
}
type DiscordChannel struct {
	RootID   string `json:"root_id"`
	Name     string `json:"name"`
	CourseID *int64 `json:"mapped_course_id"`
}

func readDiscord(ctx context.Context, db database.Operations) (Discord, error) {
	d := Discord{Interval: 3600, Threads: "All", Media: true, Parallel: 1}
	stored, err := db.Settings().LoadDiscordSettings(ctx)
	if database.IsNoRows(err) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	d.Enabled = stored.Enabled
	d.Token = identity.Decode(stored.Token)
	d.Interval = stored.Interval
	d.Threads = stored.Threads
	d.Media = stored.Media
	d.Parallel = stored.Parallel
	return d, nil
}
func (s Service) Discord(ctx context.Context) (Discord, error) { return readDiscord(ctx, s.Pool) }

func bounded(form url.Values, key string, fallback, min, max int64) (int64, error) {
	if !form.Has(key) {
		return fallback, nil
	}
	v, err := strconv.ParseInt(strings.TrimSpace(form.Get(key)), 10, 64)
	if err != nil || v < min || v > max {
		return 0, Invalid{key + " is outside the permitted integer range"}
	}
	return v, nil
}
func checked(form url.Values, key string) bool {
	return form.Get(key) == "on" || form.Get(key) == "true" || form.Get(key) == "1"
}

func (s Service) SaveDiscord(ctx context.Context, form url.Values) error {
	interval, err := bounded(form, "interval_minutes", 60, 1, 10080)
	if err != nil {
		return err
	}
	parallel, err := bounded(form, "parallel", 1, 1, 16)
	if err != nil {
		return err
	}
	threads := "All"
	if form.Has("include_threads") {
		value := strings.ToLower(strings.TrimSpace(form.Get("include_threads")))
		threads = map[string]string{"none": "None", "active": "Active", "all": "All"}[value]
		if threads == "" {
			return Invalid{"Thread policy must be None, Active, or All"}
		}
	}
	return s.mutate(ctx, "discord-export", func(tx database.Tx) error {
		old, err := readDiscord(ctx, tx)
		if err != nil {
			return err
		}
		token := old.Token
		if checked(form, "clear_token") {
			token = ""
		} else if value := strings.TrimSpace(form.Get("token")); value != "" {
			token = value
		}
		if err := tx.Settings().SaveDiscordSettings(ctx, database.SettingsDiscord{
			Enabled:  checked(form, "enabled"),
			Token:    identity.Encode(token),
			Interval: interval * 60,
			Threads:  threads,
			Media:    checked(form, "media"),
			Parallel: parallel,
		}); err != nil {
			return err
		}
		_, err = commands.EnqueueTx(ctx, tx, "discord", "reload_export", map[string]any{}, true)
		return err
	})
}

func (s Service) DiscordChannels(ctx context.Context) ([]DiscordChannel, error) {
	rows, err := s.Pool.Settings().ListDiscordChannels(ctx)
	if err != nil {
		return nil, err
	}
	channels := make([]DiscordChannel, 0, len(rows))
	for _, row := range rows {
		channels = append(channels, DiscordChannel{
			RootID:   row.RootID,
			Name:     identity.Decode(row.Name),
			CourseID: row.CourseID,
		})
	}
	return channels, nil
}

func (s Service) SaveDiscordMap(ctx context.Context, form url.Values) (int, error) {
	count := 0
	err := s.mutate(ctx, "discord-map", func(tx database.Tx) error {
		roots, err := tx.Settings().ListDiscordMappingRoots(ctx)
		if err != nil {
			return err
		}
		mapping := map[string]int64{}
		for _, root := range roots {
			raw := strings.TrimSpace(form.Get("discord_course_" + root))
			if raw == "" {
				continue
			}
			id, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || id < 1 {
				return Invalid{"Invalid eClass course selection"}
			}
			if _, err = tx.Courses().Course(ctx, id); database.IsNoRows(err) {
				return Invalid{"Unknown eClass course selection"}
			} else if err != nil {
				return err
			}
			mapping[root] = id
		}
		if err = tx.Settings().ReplaceDiscordMapping(ctx, mapping); err != nil {
			return err
		}
		count = len(mapping)
		_, err = commands.EnqueueTx(ctx, tx, "discord", "reload_messages", map[string]any{}, true)
		return err
	})
	return count, err
}
