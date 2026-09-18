package materials

import (
	"encoding/json"
	"os"
	"testing"
)

func TestClassificationFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/classification.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Path, Override, Kind, Classification string }
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		kind, source := classify(c.Path, c.Override)
		if kind != c.Kind || source != c.Classification {
			t.Errorf("%s: %s/%s", c.Path, kind, source)
		}
	}
	if aiType(map[string]string{"material_type": "past_exam"}) != "past_paper" {
		t.Fatal("AI vocabulary changed")
	}
	if aiType(map[string]string{"external_material_type": "textbook", "material_type": "past_exam"}) != "textbook" {
		t.Fatal("explicit AI category lost")
	}
}
