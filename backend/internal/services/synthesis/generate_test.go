package synthesis

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/inference"
)

type fakeStream func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error

func (f fakeStream) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	e func(inference.Delta) error,
) error {
	return f(ctx, c, r, e)
}
func fixtureJob(t *testing.T) (job, map[string]any) {
	t.Helper()
	raw, err := os.ReadFile("../../domain/blueprints/testdata/validation.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Packet, Payload map[string]any }
	if err = decode(raw, &cases); err != nil {
		t.Fatal(err)
	}
	packet := cases[0].Packet
	packet["trusted_planning_context"] = map[string]any{"target_grade": 7.5}
	packet["source_snapshot"] = map[string]any{"private": "must stay local"}
	a := settings.DefaultAI()
	a.CourseModel = "syn:fixture"
	a.CourseFallbacks = []string{"fixture-cloud"}
	return job{Lane: "course", Course: 101, Packet: packet, AI: a}, cases[0].Payload
}
func TestPromptIdentityBoundary(t *testing.T) {
	j, p := fixtureJob(t)
	before, _ := json.Marshal(j.Packet)
	request, aliases, err := prompt(j)
	if err != nil {
		t.Fatal(err)
	}
	text := request.Messages[1].Content.(string)
	if strings.Contains(text, "must stay local") || !strings.Contains(text, "E1") || aliases["E1"] != "document:one" {
		t.Fatal("prompt exposed snapshot or lost labels", text, aliases)
	}
	after, _ := json.Marshal(j.Packet)
	if string(before) != string(after) {
		t.Fatal("prompt mutated saved evidence")
	}
	raw, _ := json.Marshal(p)
	raw = []byte(strings.ReplaceAll(strings.ReplaceAll(string(raw), "document:one", "E1"), "discord:two", "E2"))
	if err = decode(raw, &p); err != nil {
		t.Fatal(err)
	}
	p["exam_strategy"].(map[string]any)["objective"] = "Greek \x00 objective"
	calls := 0
	s := Service{Keys: map[string]string{"SYNTHETIC_API_KEY": "fixture", "OLLAMA_API_KEY": "fixture"}}
	s.Generator.Client = fakeStream(
		func(_ context.Context, c inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
			calls++
			if c.Provider == "synthetic" {
				return &inference.Error{Status: 429}
			}
			raw, _ := json.Marshal(p)
			if err := emit(inference.Delta{Text: string(raw)}); err != nil {
				return err
			}
			return emit(inference.Delta{Finish: "stop"})
		},
	)
	result, err := s.generate(t.Context(), j)
	if err != nil || calls != 2 {
		t.Fatal(result, calls, err)
	}
	strategy := result.Payload["exam_strategy"].(map[string]any)
	if strategy["objective"] != "Greek  objective" || strategy["evidence_refs"].([]any)[0] != "document:one" {
		t.Fatal("canonical output", strategy)
	}
}
func TestGenerationRejectsInventedReferencesAndPartialOutput(t *testing.T) {
	for _, scenario := range []string{"unknown", "partial", "malformed"} {
		t.Run(scenario, func(t *testing.T) {
			j, p := fixtureJob(t)
			j.AI.CourseFallbacks = []string{}
			raw, _ := json.Marshal(p)
			text := strings.ReplaceAll(strings.ReplaceAll(string(raw), "document:one", "E1"), "discord:two", "E2")
			if scenario == "unknown" {
				text = strings.ReplaceAll(text, "E1", "E999")
			}
			if scenario == "malformed" {
				text = "{}"
			}
			s := Service{
				Keys: map[string]string{"SYNTHETIC_API_KEY": "fixture"},
				Generator: inference.Generator{
					Client: fakeStream(
						func(_ context.Context, _ inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
							if err := emit(inference.Delta{Text: text}); err != nil {
								return err
							}
							if scenario == "partial" {
								return nil
							}
							return emit(inference.Delta{Finish: "stop"})
						},
					),
				},
			}
			if _, err := s.generate(t.Context(), j); err == nil {
				t.Fatal("invalid response admitted")
			}
		})
	}
	j, _ := fixtureJob(t)
	s := Service{}
	if _, err := s.generate(t.Context(), j); err == nil {
		t.Fatal("missing providers succeeded")
	} else {
		var pause inference.Paused
		if !errors.As(err, &pause) {
			t.Fatal("unavailable providers should park", err)
		}
	}
}
