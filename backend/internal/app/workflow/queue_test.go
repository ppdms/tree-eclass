package workflow

import (
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/infrastructure/jobs"
)

func queueInvalidationChecks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := t.Context()
	queue := jobs.Queue{Pool: pool}
	first, err := queue.Enqueue(ctx, "synthetic-projection", "refresh", map[string]any{}, true)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := queue.Claim(ctx, "synthetic-projection")
	if err != nil || len(claimed) != 1 || claimed[0].ID != first {
		t.Fatal("claim failed", err)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			id, err := queue.Enqueue(ctx, "synthetic-projection", "refresh", map[string]any{}, true)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id
		})
	}
	wg.Wait()
	close(ids)
	second := ""
	for id := range ids {
		if id == first {
			t.Fatal("new input coalesced into an already running job")
		}
		if second != "" && id != second {
			t.Fatal("pending invalidations failed to coalesce")
		}
		second = id
	}
	if err = queue.Complete(ctx, first); err != nil {
		t.Fatal(err)
	}
	claimed, err = queue.Claim(ctx, "synthetic-projection")
	if err != nil || len(claimed) != 1 || claimed[0].ID != second {
		t.Fatal("invalidation was lost when older snapshot completed", err)
	}
	if err = queue.Complete(ctx, second); err != nil {
		t.Fatal(err)
	}
}
