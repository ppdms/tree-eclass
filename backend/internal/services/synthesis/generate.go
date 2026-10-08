package synthesis

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/integrations/inference"
)

//go:embed course-rules.txt
var courseRules string

//go:embed course-shape.json
var courseShape string

//go:embed practice-rules.txt
var practiceRules string

//go:embed practice-shape.json
var practiceShape string

func (s Service) generate(ctx context.Context, j job) (inference.Generated, error) {
	request, aliases, err := prompt(j)
	if err != nil {
		return inference.Generated{}, err
	}
	validate := func(payload map[string]any) (map[string]any, error) {
		cleanGenerated(payload)
		if err := restoreRefs(payload, aliases); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		if j.Lane == database.SynthesisPractice {
			p, err := blueprints.ValidatePractice(raw, j.Packet, j.Course, j.Unit)
			if err != nil {
				return nil, err
			}
			return asMap(p)
		}
		b, err := blueprints.Validate(raw, j.Packet)
		if err != nil {
			return nil, err
		}
		return asMap(b)
	}
	return s.Generator.Generate(ctx, inference.AnalysisCandidates(j.AI, s.Keys, j.Lane.String()), request, validate)
}
func prompt(j job) (inference.Request, map[string]string, error) {
	// Clone before relabeling. Stored identities and their hashes never change.
	packet, err := asMap(j.Packet)
	if err != nil {
		return inference.Request{}, nil, err
	}
	delete(packet, "source_snapshot")
	delete(packet, "trusted_planning_context")
	aliases, forward := map[string]string{}, map[string]string{}
	allowed := []any{}
	evidence, ok := packet["evidence"].([]any)
	if !ok || len(evidence) == 0 {
		return inference.Request{}, nil, errors.New("synthesis requires evidence")
	}
	for i, raw := range evidence {
		item, ok := raw.(map[string]any)
		if !ok {
			return inference.Request{}, nil, errors.New("invalid synthesis evidence")
		}
		ref, ok := item["evidence_ref"].(string)
		if !ok || ref == "" || forward[ref] != "" {
			return inference.Request{}, nil, errors.New("invalid synthesis reference")
		}
		label := fmt.Sprintf("E%d", i+1)
		aliases[label], forward[ref] = ref, label
		item["evidence_ref"] = label
		allowed = append(allowed, label)
	}
	packet["allowed_evidence_refs"] = allowed
	// Unit and family citations use the same short labels as their source entries.
	if err = restoreRefs(packet, forward); err != nil {
		return inference.Request{}, nil, err
	}
	rules, shape, tag, extra, limit := courseRules, courseShape, "EVIDENCE_PACKET", "", 140000
	if j.Lane == database.SynthesisPractice {
		rules, shape, tag, extra, limit = practiceRules, practiceShape, "PRACTICE_PACKET", "TARGET_QUESTION_COUNT: 8 (maximum 12; use fewer if evidence is thin)\n", 90000
	}
	raw, err := json.Marshal(packet)
	if err != nil {
		return inference.Request{}, nil, err
	}
	plan, err := json.Marshal(j.Packet["trusted_planning_context"])
	if err != nil {
		return inference.Request{}, nil, err
	}
	refs, _ := json.Marshal(allowed)
	text := "Write in English, retaining source terminology and exact formulas.\n" + extra + "OUTPUT_SCHEMA:\n" + shape + "\nALLOWED_EVIDENCE_REFS:\n" + string(
		refs,
	) + "\nTRUSTED_PLANNING_CONTEXT:\n" + string(
		plan,
	) + "\n<" + tag + ">\n" + string(
		raw,
	) + "\n</" + tag + ">"
	if utf8.RuneCountInString(text)+utf8.RuneCountInString(rules) > limit {
		return inference.Request{}, nil, errors.New("synthesis prompt exceeds bounded evidence budget")
	}
	return inference.Request{
		JSON:     true,
		Messages: []inference.Message{{Role: "system", Content: rules}, {Role: "user", Content: text}},
	}, aliases, nil
}
func restoreRefs(value any, aliases map[string]string) error {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if key == "evidence_refs" {
				refs, ok := item.([]any)
				if !ok {
					return errors.New("evidence_refs must be a list")
				}
				for i, raw := range refs {
					ref, ok := raw.(string)
					if !ok || aliases[ref] == "" {
						return errors.New("unknown synthesis evidence reference")
					}
					refs[i] = aliases[ref]
				}
			} else if err := restoreRefs(item, aliases); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := restoreRefs(item, aliases); err != nil {
				return err
			}
		}

	}
	return nil
}

func cleanGenerated(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if text, ok := item.(string); ok {
				v[key] = strings.ReplaceAll(text, "\x00", "")
			} else {
				cleanGenerated(item)
			}
		}
	case []any:
		for i, item := range v {
			if text, ok := item.(string); ok {
				v[i] = strings.ReplaceAll(text, "\x00", "")
			} else {
				cleanGenerated(item)
			}
		}
	}
}
