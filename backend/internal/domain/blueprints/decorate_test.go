package blueprints

import (
	"encoding/json"
	"os"
	"testing"
)

func TestActionIdentity(t *testing.T) {
	raw, err := os.ReadFile("testdata/identities.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Course                                     int64
		Unit, Kind, Instruction, Success, Expected string
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, item := range cases {
		a := Action{Kind: item.Kind, Instruction: item.Instruction, Success: item.Success}
		if id := ActionIdentity(item.Course, item.Unit, a); id != item.Expected {
			t.Fatalf("%q: %s != %s", item.Instruction, id, item.Expected)
		}
		a.Estimated = 99
		a.Evidence = []string{"document:new"}
		if ActionIdentity(item.Course, item.Unit, a) != item.Expected {
			t.Fatal("estimates/evidence changed action identity")
		}
	}
}

func TestDuplicateActionsKeepIndependentIDs(t *testing.T) {
	raw, err := os.ReadFile("testdata/validation.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture []struct {
		Payload json.RawMessage
		Packet  map[string]any
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	b, err := Validate(fixture[0].Payload, fixture[0].Packet)
	if err != nil {
		t.Fatal(err)
	}
	b.Units[0].Actions = append(b.Units[0].Actions, b.Units[0].Actions[0])
	view, err := Decorate(b, 101, "Course", "revision", nil)
	if err != nil {
		t.Fatal(err)
	}
	actions := view["units"].([]any)[0].(map[string]any)["actions"].([]any)
	first := actions[0].(map[string]any)["action_id"].(string)
	if actions[1].(map[string]any)["action_id"] != first+":2" {
		t.Fatal("duplicate action collision")
	}
}
