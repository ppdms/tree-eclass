package blueprints

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestPracticeValidation(t *testing.T) {
	raw, err := os.ReadFile("testdata/practice.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name     string
		Payload  json.RawMessage
		Packet   map[string]any
		Valid    bool
		Expected any
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, item := range cases {
		t.Run(item.Name, func(t *testing.T) {
			result, err := ValidatePractice(item.Payload, item.Packet, 101, "unit_one")
			if (err == nil) != item.Valid {
				t.Fatal("validation", err)
			}
			if !item.Valid {
				return
			}
			raw, _ := json.Marshal(result)
			var actual any
			if err = json.Unmarshal(raw, &actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, item.Expected) {
				t.Fatalf("normalization mismatch: %#v", actual)
			}
		})
	}
}

func TestQuestionIdentity(t *testing.T) {
	raw, err := os.ReadFile("testdata/question-identities.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Prompt, Expected string }
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, item := range cases {
		if id := QuestionIdentity(9007199254740993, "unit_one", item.Prompt, "short_answer"); id != item.Expected {
			t.Fatalf("%q: %s != %s", item.Prompt, id, item.Expected)
		}
	}
}
