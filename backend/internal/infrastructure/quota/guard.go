package quota

import (
	"context"
	"errors"
	"sync"
	"time"

	"tree-eclass/internal/integrations/inference"
)

type Store interface {
	Load(context.Context, string) (State, error)
	Save(context.Context, string, State) error
}
type Streamer interface {
	Stream(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error
}
type providerState struct {
	mu     sync.Mutex
	loaded bool
	value  State
}
type Guard struct {
	Client    Streamer
	Probe     Probe
	Store     Store
	Now       func() time.Time
	mu        sync.Mutex
	providers map[string]*providerState
}

func (g *Guard) state(name string) *providerState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.providers == nil {
		g.providers = map[string]*providerState{}
	}
	if g.providers[name] == nil {
		g.providers[name] = &providerState{}
	}
	return g.providers[name]
}
func (g *Guard) now() time.Time {
	if g.Now != nil {
		return g.Now().UTC()
	}
	return time.Now().UTC()
}
func blocked(c inference.Candidate, s State, now time.Time) error {
	delay := time.Minute
	if s.Blocked != nil {
		delay = max(0, s.Blocked.Sub(now))
	}
	return &inference.Error{
		Provider:   c.Provider,
		Status:     429,
		Retryable:  true,
		RetryAfter: delay,
		Reason:     "provider quota is paused",
	}
}
func (g *Guard) before(ctx context.Context, c inference.Candidate) error {
	p := g.state(c.Provider)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := g.now()
	if !p.loaded {
		if err := g.hydrate(ctx, c, p, now); err != nil {
			return err
		}
	}
	if p.value.Blocked != nil && p.value.Blocked.After(now) {
		return blocked(c, p.value, now)
	}
	if p.value.Checked.IsZero() || !p.value.Next.After(now) || p.value.Requests >= 20 {
		next, err := g.recheck(ctx, c, p.value, now)
		if err != nil {
			return err
		}
		p.value = next
	}
	if p.value.Blocked == nil {
		p.value = advanceWindow(p.value, now)
	}
	if err := g.Store.Save(ctx, c.Provider, p.value); err != nil {
		return errors.New("could not persist provider quota admission")
	}
	if p.value.Blocked != nil {
		return blocked(c, p.value, now)
	}
	return nil
}

// hydrate loads persisted quota state once per process. It never reuses a
// previous process's available snapshot; persisted pauses survive restarts,
// including upstream Retry-After responses.
func (g *Guard) hydrate(ctx context.Context, c inference.Candidate, p *providerState, now time.Time) error {
	saved, err := g.Store.Load(ctx, c.Provider)
	if err != nil {
		return errors.New("could not read provider quota state")
	}
	if saved.Blocked != nil && saved.Blocked.After(now) {
		p.value = saved
	}
	p.loaded = true
	return nil
}

// recheck probes tracked providers and re-evaluates the admission window.
func (g *Guard) recheck(ctx context.Context, c inference.Candidate, value State, now time.Time) (State, error) {
	next := evaluate(Snapshot{}, now)
	if c.Provider == "synthetic" || c.Provider == "ollama" {
		snapshot, err := g.Probe.Fetch(ctx, c)
		if ctx.Err() != nil {
			return value, ctx.Err()
		}
		if err != nil {
			delay := 5 * time.Minute
			var failure *inference.Error
			if errors.As(err, &failure) && failure.RetryAfter > 0 {
				delay = failure.RetryAfter
			}
			next = paused("check_failed", "Quota could not be checked; provider requests are paused.", now, delay)
		} else {
			next = evaluate(snapshot, now)
		}
	}
	return next, nil
}

// advanceWindow counts the admitted request and forces a recheck before
// exhausting a reported request-count window; the server is authoritative
// about regeneration and other clients' usage.
func advanceWindow(value State, now time.Time) State {
	value.Requests++
	for _, window := range value.Windows {
		if window.Remaining != nil && float64(value.Requests) >= *window.Remaining {
			value.Next = now
		}
	}
	return value
}

func (g *Guard) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	emit func(inference.Delta) error,
) error {
	if err := g.before(ctx, c); err != nil {
		return err
	}
	err := g.Client.Stream(ctx, c, r, emit)
	var failure *inference.Error
	if !errors.As(err, &failure) || failure.Status != 429 {
		return err
	}
	p := g.state(c.Provider)
	p.mu.Lock()
	defer p.mu.Unlock()
	delay := failure.RetryAfter
	if delay <= 0 {
		delay = 330 * time.Second
	}
	p.value = paused("rate_limited", "The provider returned a rate limit; waiting before rechecking.", g.now(), delay)
	if saveErr := g.Store.Save(ctx, c.Provider, p.value); saveErr != nil {
		return errors.New("provider rate limit could not be persisted")
	}
	return err
}
