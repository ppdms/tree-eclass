package blueprints

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

var keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var priorities = []string{"critical", "high", "medium", "low"}
var confidences = []string{"high", "medium", "low"}

// Validate binds every citation to an explicitly admitted packet entry. The
// wire decoder also rejects missing, null, unknown and incorrectly typed fields.
func Validate(raw []byte, packet map[string]any) (Blueprint, error) {
	result := Blueprint{}
	known, err := packetRefs(packet)
	if err != nil {
		return result, err
	}
	if len(raw) > 8*1024*1024 {
		return result, errors.New("blueprint exceeds 8 MiB")
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	if len(result.Units) < 1 || len(result.Units) > 200 || len(result.Families) > 200 || len(result.Conflicts) > 200 ||
		len(result.Gaps) > 200 {
		return result, errors.New("blueprint section exceeds its bounds")
	}
	units := map[string]bool{}
	for i := range result.Units {
		if err = result.Units[i].validate(known, units); err != nil {
			return result, fmt.Errorf("unit %d: %w", i, err)
		}
		units[result.Units[i].Key] = true
	}
	if err = result.Strategy.validate(known, units); err != nil {
		return result, err
	}
	if err = result.validateCollections(known, units); err != nil {
		return result, err
	}
	return result, nil
}

func text(value *string, maximum int) error {
	*value = strings.TrimSpace(*value)
	if *value == "" || utf8.RuneCountInString(*value) > maximum {
		return fmt.Errorf("text requires 1 to %d characters", maximum)
	}
	return nil
}
func key(value *string) error {
	if err := text(value, 64); err != nil {
		return err
	}
	if !keyPattern.MatchString(*value) {
		return errors.New("invalid blueprint key")
	}
	return nil
}
func enum(value string, allowed []string) error {
	if !slices.Contains(allowed, value) {
		return fmt.Errorf("invalid blueprint category %q", value)
	}
	return nil
}
func textList(values []string, minimum, maximum int, keys bool) error {
	if values == nil || len(values) < minimum || len(values) > maximum {
		return errors.New("invalid blueprint list length")
	}
	seen := map[string]bool{}
	for i := range values {
		var err error
		if keys {
			err = key(&values[i])
		} else {
			err = text(&values[i], 300)
		}
		if err != nil {
			return err
		}
		if seen[values[i]] {
			return errors.New("duplicate blueprint list value")
		}
		seen[values[i]] = true
	}
	return nil
}
func references(values []string, known map[string]bool, minimum int) error {
	if err := textList(values, minimum, 50, false); err != nil {
		return err
	}
	for _, ref := range values {
		if !known[ref] {
			return fmt.Errorf("unknown evidence reference %q", ref)
		}
	}
	return nil
}

func resolveKeys(values []string, known map[string]bool) []string {
	normalize := func(v string) string { return strings.Trim(strings.ReplaceAll(v, "-", "_"), "_") }
	result := []string{}
	for _, value := range values {
		match := ""
		if known[value] {
			match = value
		} else {
			exact, partial := []string{}, []string{}
			target := normalize(value)
			for candidate := range known {
				normal := normalize(candidate)
				if normal == target {
					exact = append(exact, candidate)
				}
				if strings.HasPrefix(normal, target) || strings.HasPrefix(target, normal) {
					partial = append(partial, candidate)
				}
			}
			if len(exact) == 1 {
				match = exact[0]
			} else if len(partial) == 1 {
				match = partial[0]
			}
		}
		if match != "" && !slices.Contains(result, match) {
			result = append(result, match)
		}
	}
	return result
}

func packetRefs(packet map[string]any) (map[string]bool, error) {
	raw, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Evidence []struct {
			Ref string `json:"evidence_ref"`
		} `json:"evidence"`
		Allowed []string `json:"allowed_evidence_refs"`
	}
	if err = json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	if wire.Evidence == nil || wire.Allowed == nil {
		return nil, errors.New("packet must declare admitted evidence")
	}
	known := map[string]bool{}
	for _, item := range wire.Evidence {
		ref := item.Ref
		if err = text(&ref, 300); err != nil {
			return nil, err
		}
		if known[ref] {
			return nil, errors.New("duplicate packet evidence")
		}
		known[ref] = true
	}
	seen := map[string]bool{}
	for _, ref := range wire.Allowed {
		if !known[ref] || seen[ref] {
			return nil, errors.New("allowed references do not match admitted evidence")
		}
		seen[ref] = true
	}
	if len(seen) != len(known) {
		return nil, errors.New("allowed references do not cover admitted evidence")
	}
	return known, nil
}
