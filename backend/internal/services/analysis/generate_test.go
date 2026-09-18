package analysis

import (
	"context"
	"errors"
	"testing"
	"time"

	"tree-eclass/internal/integrations/inference"
)

type streamFunc func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error

func (f streamFunc) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	emit func(inference.Delta) error,
) error {
	return f(ctx, c, r, emit)
}
func TestGeneratorRejectsPartialAndMalformedBeforeFallback(t *testing.T) {
	calls := 0
	g := inference.Generator{
		Client: streamFunc(
			func(_ context.Context, c inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
				calls++
				if c.Model == "first" {
					return emit(inference.Delta{Text: `{"summary":"partial"}`})
				}
				if c.Model == "second" {
					return emit(inference.Delta{Text: `{"wrong":"shape"}`, Finish: "stop"})
				}
				return emit(inference.Delta{Text: `{"summary":"complete"}`, Finish: "stop"})
			},
		),
	}
	result, err := g.Generate(
		t.Context(),
		[]inference.Candidate{{Model: "first"}, {Model: "second"}, {Model: "third"}},
		inference.Request{},
		validateDocument,
	)
	if err != nil || result.Model != "third" || result.Payload["summary"] != "complete" || calls != 3 {
		t.Fatal(result, calls, err)
	}
}
func TestGeneratorQuotaPauseDoesNotBecomePermanentFailure(t *testing.T) {
	g := inference.Generator{
		Client: streamFunc(
			func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
				return &inference.Error{Status: 429, Retryable: true, RetryAfter: time.Minute}
			},
		),
	}
	_, err := g.Generate(t.Context(), []inference.Candidate{{Model: "one"}}, inference.Request{}, validateDocument)
	var pause inference.Paused
	if !errors.As(err, &pause) || pause.Delay != time.Minute {
		t.Fatal(err)
	}
}
