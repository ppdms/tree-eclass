package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/integrations/discord"
)

func (s *Server) discordMedia(w http.ResponseWriter, r *http.Request) {
	object, err := (messages.Reader{Pool: s.db.Pool}).Media(r.Context(), r.PathValue("object_id"))
	if err != nil {
		s.fileError(w, err)
		return
	}
	s.blobs.ServeDownload(w, r, object, "discord-attachment")
}
func (s *Server) discordWorker(ctx context.Context) error {
	service := discord.Service{
		Pool:    s.db.Pool,
		Objects: s.blobs,
		Runner: discord.NativeRunner{
			Binary:   s.config.DiscordExporter,
			Registry: filepath.Join(s.config.Temp, ".helpers"),
		},
		Temp: s.config.Temp,
	}
	importer := messages.Importer{Pool: s.db.Pool, Blobs: s.blobs, Temp: s.config.Temp}
	queue := jobs.Queue{Pool: s.db.Pool}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	next := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if now.Before(next) || !s.expensiveMu.TryLock() {
				continue
			}
			err := s.discordOnce(ctx, service, importer, queue, now)
			s.expensiveMu.Unlock()
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				slog.Error("Discord processing failed", "error", err)
				next = now.Add(time.Minute)
			}
		}
	}
}

func (s *Server) discordOnce(
	ctx context.Context,
	service discord.Service,
	importer messages.Importer,
	queue jobs.Queue,
	now time.Time,
) error {
	if worked, err := importer.ReindexMapped(ctx); worked || err != nil {
		return err
	}
	if !s.config.ExternalWorkers {
		return nil
	}
	commands, err := queue.Claim(ctx, "discord")
	if err != nil {
		return err
	}
	force := false
	for _, command := range commands {
		switch command.Action {
		case "reload_export":
			force = true
		case "reload_messages":
		default:
			err = errors.New("unknown Discord control action")
		}
		if err == nil {
			err = service.Tick(ctx, now, force)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return errors.Join(err, queue.Fail(ctx, command, err))
		}
		return queue.Complete(ctx, command.ID)
	}
	return service.Tick(ctx, now, false)
}
