package blueprints

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// ActionIdentity deliberately excludes ordering, estimates and citations: edits
// to those fields must not erase completed work on the same learning activity.
func ActionIdentity(course int64, unit string, action Action) string {
	value := map[string]any{"course_id": course, "unit_key": unit, "kind": action.Kind,
		"instruction": action.Instruction, "success_criterion": action.Success}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
	data := bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
	// encoding/json escapes JavaScript line separators even in non-HTML mode.
	// The identity contract hashes their literal UTF-8 encoding.
	data = literalSeparators(data)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("course-blueprint:%d:%s:%x", course, unit, hash[:12])
}

func literalSeparators(data []byte) []byte {
	result := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] == '\\' && i+1 < len(data) {
			if i+6 <= len(data) && (string(data[i:i+6]) == `\u2028` || string(data[i:i+6]) == `\u2029`) {
				if data[i+5] == '8' {
					result = append(result, []byte("\u2028")...)
				} else {
					result = append(result, []byte("\u2029")...)
				}
				i += 5
				continue
			}
			result = append(result, data[i], data[i+1])
			i++
			continue
		}
		result = append(result, data[i])
	}
	return result
}

// Decorate adds immutable UI/action membership to an already validated plan.
// Progress is joined independently when navigation is read.
func Decorate(
	b Blueprint,
	course int64,
	name, revision string,
	evidence map[string]map[string]any,
) (map[string]any, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&result); err != nil {
		return nil, err
	}
	occurrences := map[string]int{}
	units := result["units"].([]any)
	for i, unit := range b.Units {
		row := units[i].(map[string]any)
		row["evidence_links"] = resolve(unit.Evidence, evidence)
		actions := row["actions"].([]any)
		for j, action := range unit.Actions {
			id := ActionIdentity(course, unit.Key, action)
			occurrences[id]++
			if occurrences[id] > 1 {
				id = fmt.Sprintf("%s:%d", id, occurrences[id])
			}
			value := actions[j].(map[string]any)
			value["id"], value["action_id"] = id, id
			value["action_type"], value["title"] = action.Kind, action.Instruction
			value["objective"], value["rationale"] = unit.Objective, action.Success
			value["course_id"], value["course_name"] = course, name
			value["unit_key"], value["unit_title"] = unit.Key, unit.Title
			value["blueprint_revision_id"], value["plan_revision"] = revision, revision
			value["evidence_links"] = resolve(action.Evidence, evidence)
		}
	}
	result["exam_strategy"].(map[string]any)["evidence_links"] = resolve(b.Strategy.Evidence, evidence)
	for _, key := range []string{"question_families", "conflicts", "coverage_gaps"} {
		for _, item := range result[key].([]any) {
			row := item.(map[string]any)
			refs := []string{}
			for _, ref := range row["evidence_refs"].([]any) {
				refs = append(refs, ref.(string))
			}
			row["evidence_links"] = resolve(refs, evidence)
		}
	}
	return result, nil
}

func resolve(refs []string, evidence map[string]map[string]any) []map[string]any {
	result := []map[string]any{}
	for _, ref := range refs {
		value, ok := evidence[ref]
		if !ok {
			value = map[string]any{
				"evidence_ref":      ref,
				"label":             "Unavailable evidence",
				"available":         false,
				"untrusted_content": true,
			}
		}
		result = append(result, value)
	}
	return result
}
