package process

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// RSS admission is sampled, not a kernel hard cap. Source, expansion and output
// limits remain mandatory even when a transient allocation precedes a sample.
func WatchMemory(ctx context.Context, pid int, limit int64) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		rss, err := DescendantRSS(ctx, pid)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return fmt.Errorf("cannot supervise helper memory: %w", err)
		}
		if rss > limit {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return fmt.Errorf("helper process tree exceeded %d MiB RSS", limit/(1024*1024))
		}
	}
}
func DescendantRSS(ctx context.Context, root int) (int64, error) {
	cmd := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,rss=")
	data, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	type entry struct {
		pid, parent int
		rss         int64
	}
	entries := []entry{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		parent, _ := strconv.Atoi(fields[1])
		rss, _ := strconv.ParseInt(fields[2], 10, 64)
		entries = append(entries, entry{pid, parent, rss * 1024})
	}
	owned := map[int]bool{root: true}
	for changed := true; changed; {
		changed = false
		for _, entry := range entries {
			if owned[entry.parent] && !owned[entry.pid] {
				owned[entry.pid] = true
				changed = true
			}
		}
	}
	var total int64
	for _, entry := range entries {
		if owned[entry.pid] {
			total += entry.rss
		}
	}
	return total, nil
}
