package inference

import (
	"context"
	"errors"
	"strings"
	"time"
)

type Streamer interface {
	Stream(context.Context, Candidate, Request, func(Delta) error) error
}
type Generator struct{ Client Streamer }
type Generated struct {
	Model, Provider string
	Payload         map[string]any
}
type Paused struct{ Delay time.Duration }

func (p Paused) Error() string { return "all analysis providers are paused or unavailable" }

func (g Generator) Generate(
	ctx context.Context,
	candidates []Candidate,
	in Request,
	validate func(map[string]any) (map[string]any, error),
) (Generated, error) {
	paused := true
	delay := 5 * time.Minute
	for _, candidate := range candidates {
		var output strings.Builder
		finished := false
		err := g.Client.Stream(ctx, candidate, in, func(d Delta) error {
			if output.Len()+len(d.Text) > 1024*1024 {
				return errors.New("analysis output exceeds 1 MiB")
			}
			output.WriteString(d.Text)
			if len(d.Calls) > 0 {
				return errors.New("analysis cannot request tool execution")
			}
			if d.Finish != "" {
				finished = true
			}
			return nil
		})
		if ctx.Err() != nil {
			return Generated{}, ctx.Err()
		}
		var failure *Error
		if errors.As(err, &failure) && failure.Status == 429 {
			if failure.RetryAfter > 0 {
				delay = min(delay, failure.RetryAfter)
			}
			continue
		}
		paused = false
		if err != nil || !finished {
			continue
		}
		p, err := ParseObject(output.String())
		if err != nil {
			continue
		}
		payload, err := validate(p)
		if err != nil {
			continue
		}
		return Generated{Model: candidate.Model, Provider: candidate.Provider, Payload: payload}, nil
	}
	if paused {
		return Generated{}, Paused{Delay: max(time.Second, delay)}
	}
	return Generated{}, errors.New("no configured model produced a valid complete analysis")
}
