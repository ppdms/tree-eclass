package discord

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
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
	channels := make([]database.DiscoveredChannel, 0, len(all))
	for _, ch := range all {
		channels = append(channels, database.DiscoveredChannel{
			ChannelID: ch.ID,
			RootID:    ch.Root,
			GuildID:   ch.Guild,
			Name:      identity.Encode(ch.Name),
			IsThread:  ch.Thread,
		})
	}
	if err = tx.Discord().ReplaceDiscovery(ctx, channels); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
