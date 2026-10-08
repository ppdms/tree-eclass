package settings

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
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

func readDiscord(ctx context.Context, db queryer) (Discord, error) {
	d := Discord{Interval: 3600, Threads: "All", Media: true, Parallel: 1}
	err := db.QueryRow(ctx, `SELECT enabled=1,token,interval_seconds,include_threads,media=1,parallel FROM app.discord_export_settings WHERE id=1`).
		Scan(&d.Enabled, &d.Token, &d.Interval, &d.Threads, &d.Media, &d.Parallel)
	if errors.Is(err, rdbms.ErrNoRows) {
		err = nil
	}
	d.Token = identity.Decode(d.Token)
	return d, err
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
	return s.mutate(ctx, "discord-export", func(tx rdbms.Tx) error {
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
		_, err = tx.Exec(
			ctx,
			`INSERT INTO app.discord_export_settings(id,enabled,token,interval_seconds,include_threads,media,parallel) VALUES(1,$1,$2,$3,$4,$5,$6)
ON CONFLICT(id) DO UPDATE SET enabled=$1,token=$2,interval_seconds=$3,include_threads=$4,media=$5,parallel=$6,updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
			flag(checked(form, "enabled")),
			identity.Encode(token),
			interval*60,
			threads,
			flag(checked(form, "media")),
			parallel,
		)
		if err != nil {
			return err
		}
		_, err = commands.EnqueueTx(ctx, tx, "discord", "reload_export", map[string]any{}, true)
		return err
	})
}

func (s Service) DiscordChannels(ctx context.Context) ([]DiscordChannel, error) {
	rows, err := s.Pool.Query(
		ctx,
		`SELECT coalesce(r.root_channel_id,m.root_channel_id),coalesce(r.name,'Unavailable channel'),m.course_id FROM app.discord_root_channels r FULL JOIN app.discord_course_channels m ON m.root_channel_id=r.root_channel_id ORDER BY lower(coalesce(r.name,'Unavailable channel')),coalesce(r.root_channel_id,m.root_channel_id)`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	channels := []DiscordChannel{}
	for rows.Next() {
		var c DiscordChannel
		if err = rows.Scan(&c.RootID, &c.Name, &c.CourseID); err != nil {
			return nil, err
		}
		c.Name = identity.Decode(c.Name)
		channels = append(channels, c)
	}
	return channels, rows.Err()
}

func (s Service) SaveDiscordMap(ctx context.Context, form url.Values) (int, error) {
	count := 0
	err := s.mutate(ctx, "discord-map", func(tx rdbms.Tx) error {
		rows, err := tx.Query(
			ctx,
			`SELECT root_channel_id FROM app.discord_root_channels UNION SELECT root_channel_id FROM app.discord_course_channels`,
		)
		if err != nil {
			return err
		}
		var roots []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				return err
			}
			roots = append(roots, v)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
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
			var exists bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.courses WHERE id=$1)`, id).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return Invalid{"Unknown eClass course selection"}
			}
			mapping[root] = id
		}
		if _, err = tx.Exec(ctx, `DELETE FROM app.discord_course_channels`); err != nil {
			return err
		}
		for root, id := range mapping {
			if _, err = tx.Exec(ctx, `INSERT INTO app.discord_course_channels(root_channel_id,course_id) VALUES($1,$2)`, root, id); err != nil {
				return err
			}
		}
		count = len(mapping)
		_, err = commands.EnqueueTx(ctx, tx, "discord", "reload_messages", map[string]any{}, true)
		return err
	})
	return count, err
}
