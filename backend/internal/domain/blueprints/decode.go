package blueprints

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func strict(raw []byte, target any, required string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errors.New("blueprint section must be an object")
	}
	for _, key := range strings.Fields(required) {
		value, found := fields[key]
		if !found || bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("blueprint section requires %s", key)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func (b *Blueprint) UnmarshalJSON(raw []byte) error {
	type plain Blueprint
	wire := struct {
		*plain
		Version json.RawMessage `json:"blueprint_version"`
	}{plain: (*plain)(b)}
	if err := strict(raw, &wire, "exam_strategy units question_families conflicts coverage_gaps"); err != nil {
		return err
	}
	if len(wire.Version) > 0 {
		var version string
		if json.Unmarshal(wire.Version, &version) != nil || version != "course-blueprint-v1" {
			return errors.New("unsupported blueprint version")
		}
	}
	b.Version = "course-blueprint-v1"
	return nil
}
func (s *Strategy) UnmarshalJSON(raw []byte) error {
	type plain Strategy
	return strict(
		raw,
		(*plain)(s),
		"objective approach target_score_percent confidence priority_unit_keys evidence_refs",
	)
}
func (u *Unit) UnmarshalJSON(raw []byte) error {
	type plain Unit
	// A reported unit duration is ignored; it is derived from validated actions.
	wire := struct {
		*plain
		Estimated json.RawMessage `json:"estimated_minutes"`
	}{plain: (*plain)(u)}
	return strict(raw, &wire, "key title objective priority depends_on evidence_refs actions")
}
func (a *Action) UnmarshalJSON(raw []byte) error {
	type plain Action
	return strict(raw, (*plain)(a), "kind instruction estimated_minutes success_criterion evidence_refs")
}
func (f *Family) UnmarshalJSON(raw []byte) error {
	type plain Family
	return strict(
		raw,
		(*plain)(f),
		"key name response_mode priority confidence observed_count observed_out_of estimated_marks_percent unit_keys evidence_refs",
	)
}
func (c *Conflict) UnmarshalJSON(raw []byte) error {
	type plain Conflict
	return strict(raw, (*plain)(c), "summary status resolution evidence_refs")
}
func (g *Gap) UnmarshalJSON(raw []byte) error {
	type plain Gap
	return strict(raw, (*plain)(g), "summary severity recommended_action evidence_refs")
}
