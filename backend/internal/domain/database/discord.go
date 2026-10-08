package database

import (
	"context"
	"time"
)

// DiscoveredChannel is one validated exporter listing row. Name stays encoded
// exactly as stored; callers decode via identity.Decode.
type DiscoveredChannel struct {
	ChannelID int64
	RootID    int64
	GuildID   int64
	Name      string
	IsThread  bool
}

// ExportSource is the next due Discord export interval: mapped root channel,
// member channel, owning course and last published cursor.
type ExportSource struct {
	Root    int64
	Channel int64
	Course  int64
	After   int64
}

// DiscordExportFailure records a failed interval retry without advancing
// the cursor. NextAt is the retry instant; Error is stored verbatim.
type DiscordExportFailure struct {
	Root    int64
	Channel int64
	After   int64
	NextAt  time.Time
	Error   string
}

// Discord is the typed port for native exporter discovery state and export
// cursor scheduling. Discovery replaces the full discovered/root snapshot
// atomically; cursor reads join only mapped channels.
type Discord interface {
	// DiscoveryDue reports whether native discovery must run: no ready
	// marker, or the marker is older than the freshness window.
	DiscoveryDue(ctx context.Context) (bool, error)
	// ReplaceDiscovery atomically swaps the discovered/root snapshot and
	// marks native discovery ready on the backend clock.
	ReplaceDiscovery(ctx context.Context, channels []DiscoveredChannel) error
	// NextExportSource returns the oldest due mapped export interval,
	// ordered by next retry (never attempted first), then channel.
	// It reports ErrNoRows when no interval is due.
	NextExportSource(ctx context.Context, now time.Time) (ExportSource, error)
	// RecordExportFailure preserves the cursor while scheduling a retry
	// and recording the failure message.
	RecordExportFailure(ctx context.Context, failure DiscordExportFailure) error
}
