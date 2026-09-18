package discord

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type Channel struct {
	ID, Root, Guild int64
	Name            string
	Thread          bool
}

var ansi = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

func listing(output string, threads bool) ([]Channel, error) {
	channels := []Channel{}
	var parent int64
	seen := map[int64]bool{}
	for _, line := range strings.Split(ansi.ReplaceAllString(output, ""), "\n") {
		line = strings.TrimSpace(line)
		thread := strings.HasPrefix(line, "*")
		if thread {
			line = strings.TrimSpace(strings.TrimPrefix(line, "*"))
		}
		left, name, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSpace(left), 10, 64)
		if err != nil {
			continue
		}
		if id == 0 {
			continue
		}
		if id < 0 || seen[id] {
			return nil, errors.New("invalid or duplicate Discord listing identity")
		}
		if thread && (!threads || parent == 0) {
			return nil, errors.New("Discord listing has a thread without its parent")
		}
		if !thread {
			parent = id
		}
		name = strings.TrimSpace(name)
		if name == "" || len(name) > 4096 {
			return nil, errors.New("invalid Discord channel name")
		}
		channels = append(channels, Channel{ID: id, Root: parent, Name: name, Thread: thread})
		seen[id] = true
		if len(channels) > 20000 {
			return nil, errors.New("Discord listing exceeds 20000 entries")
		}
	}
	return channels, nil
}
func (s Service) discover(ctx context.Context, dir string, cfg settings.Discord) error {
	output, err := s.Runner.Run(ctx, dir, cfg.Token, "guilds")
	if err != nil {
		return err
	}
	guilds, err := listing(output, false)
	if err != nil {
		return err
	}
	if len(guilds) > 200 {
		return errors.New("Discord discovery exceeds 200 guilds")
	}
	all := []Channel{}
	for _, guild := range guilds {
		channels, err := s.guildChannels(ctx, dir, cfg, guild)
		if err != nil {
			return err
		}
		all = append(all, channels...)
		if len(all) > 20000 {
			return errors.New("Discord discovery exceeds 20000 channels")
		}
	}
	return s.storeDiscovered(ctx, cfg, all)
}

func (s Service) guildChannels(
	ctx context.Context,
	dir string,
	cfg settings.Discord,
	guild Channel,
) ([]Channel, error) {
	output, err := s.Runner.Run(
		ctx,
		dir,
		cfg.Token,
		"channels",
		"--guild",
		fmt.Sprint(guild.ID),
		"--include-vc",
		"false",
		"--include-threads",
		cfg.Threads,
	)
	if err != nil {
		return nil, err
	}
	channels, err := listing(output, true)
	if err != nil {
		return nil, err
	}
	for i := range channels {
		channels[i].Guild = guild.ID
		channels[i].Name = guild.Name + " / " + channels[i].Name
	}
	return channels, nil
}

func (s Service) storeDiscovered(ctx context.Context, cfg settings.Discord, all []Channel) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = checkSettings(ctx, tx, cfg); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM app.discord_discovered_channels;DELETE FROM app.discord_root_channels`); err != nil {
		return err
	}
	for _, ch := range all {
		if _, err = tx.Exec(ctx, `INSERT INTO app.discord_discovered_channels(channel_id,root_id,guild_id,name,is_thread) VALUES($1,$2,$3,$4,$5)`, ch.ID, ch.Root, ch.Guild, identity.Encode(ch.Name), ch.Thread); err != nil {
			return err
		}
		if !ch.Thread {
			if _, err = tx.Exec(ctx, `INSERT INTO app.discord_root_channels(root_channel_id,name,metadata) VALUES($1,$2,jsonb_build_object('guild_id',$3::text))`, fmt.Sprint(ch.ID), identity.Encode(ch.Name), fmt.Sprint(ch.Guild)); err != nil {
				return err
			}
		}
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO messages.message_state(key,value,updated_at) VALUES('native_discovery','ready',clock_timestamp()::text) ON CONFLICT(key) DO UPDATE SET value='ready',updated_at=excluded.updated_at`,
	)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
