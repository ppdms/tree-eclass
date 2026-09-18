package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"tree-eclass/internal/integrations/inference"
)

type memoryStore struct {
	mu      sync.Mutex
	values  map[string]State
	failure bool
}

func (s *memoryStore) Load(_ context.Context, key string) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key], nil
}
func (s *memoryStore) Save(_ context.Context, key string, value State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure {
		return errors.New("fixture write failed")
	}
	s.values[key] = value
	return nil
}

type probeFunc func(context.Context, inference.Candidate) (Snapshot, error)

func (f probeFunc) Fetch(ctx context.Context, c inference.Candidate) (Snapshot, error) {
	return f(ctx, c)
}

type streamFunc func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error

func (f streamFunc) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	emit func(inference.Delta) error,
) error {
	return f(ctx, c, r, emit)
}
func fixtureGuard(now *time.Time) (*Guard, *memoryStore, *int, *int) {
	store := &memoryStore{values: map[string]State{}}
	checks, calls := 0, 0
	guard := &Guard{
		Store: store,
		Now:   func() time.Time { return *now },
		Probe: probeFunc(
			func(context.Context, inference.Candidate) (Snapshot, error) { checks++; return Snapshot{}, nil },
		),
		Client: streamFunc(
			func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
				calls++
				return nil
			},
		),
	}
	return guard, store, &checks, &calls
}
func TestQuotaCacheRateLimitAndRestart(t *testing.T) {
	now := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	g, store, checks, calls := fixtureGuard(&now)
	c := inference.Candidate{Provider: "synthetic"}
	for range 20 {
		if err := g.Stream(t.Context(), c, inference.Request{}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if *checks != 1 || *calls != 20 {
		t.Fatal(*checks, *calls)
	}
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err != nil || *checks != 2 {
		t.Fatal("request-budget recheck", err, *checks)
	}
	g.Client = streamFunc(
		func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
			return &inference.Error{Status: 429, Retryable: true, RetryAfter: 10 * time.Minute}
		},
	)
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err == nil {
		t.Fatal("429 ignored")
	}
	fresh, _, freshChecks, freshCalls := fixtureGuard(&now)
	fresh.Store = store
	if err := fresh.Stream(t.Context(), c, inference.Request{}, nil); err == nil || *freshChecks != 0 ||
		*freshCalls != 0 {
		t.Fatal("restart erased provider pause", err)
	}
	now = now.Add(10 * time.Minute)
	if err := fresh.Stream(t.Context(), c, inference.Request{}, nil); err != nil || *freshChecks != 1 ||
		*freshCalls != 1 {
		t.Fatal("reset did not require fresh check", err)
	}
}
func TestQuotaFailureAndCanceledProbeRemainClosed(t *testing.T) {
	now := time.Now()
	g, store, _, calls := fixtureGuard(&now)
	c := inference.Candidate{Provider: "ollama"}
	g.Probe = probeFunc(func(context.Context, inference.Candidate) (Snapshot, error) {
		return Snapshot{}, errors.New("private fixture")
	})
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err == nil || *calls != 0 {
		t.Fatal("failed quota check admitted model")
	}
	if store.values["ollama"].Status != "check_failed" {
		t.Fatal(store.values)
	}
	now = now.Add(5 * time.Minute)
	canceled, cancel := context.WithCancel(t.Context())
	g.Probe = probeFunc(
		func(context.Context, inference.Candidate) (Snapshot, error) { cancel(); return Snapshot{}, nil },
	)
	if err := g.Stream(canceled, c, inference.Request{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	probes := 0
	g.Probe = probeFunc(
		func(context.Context, inference.Candidate) (Snapshot, error) { probes++; return Snapshot{}, nil },
	)
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err != nil || probes != 1 {
		t.Fatal("canceled probe cached as available", err, probes)
	}
	store.failure = true
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err == nil || *calls != 1 {
		t.Fatal("unpersisted admission made a model call", err, *calls)
	}
}
func TestUnmeteredProviderStillHonorsRateLimit(t *testing.T) {
	now := time.Now()
	g, _, checks, _ := fixtureGuard(&now)
	g.Client = streamFunc(
		func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
			return &inference.Error{Status: 429, Retryable: true}
		},
	)
	c := inference.Candidate{Provider: "huggingface"}
	_ = g.Stream(t.Context(), c, inference.Request{}, nil)
	if err := g.Stream(t.Context(), c, inference.Request{}, nil); err == nil || *checks != 0 {
		t.Fatal("untracked provider rate limit bypassed", err)
	}
}
