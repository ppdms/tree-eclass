package blueprints

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestBlueprintValidationFixture(t *testing.T) {
	// The fixture is side-effect free and does not open storage. Invalid provider
	// output must remain invalid.
	raw, err := os.ReadFile("testdata/validation.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name     string
		Payload  json.RawMessage
		Packet   map[string]any
		Valid    bool
		Expected any
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			result, err := Validate(fixture.Payload, fixture.Packet)
			if (err == nil) != fixture.Valid {
				t.Fatalf("valid=%v, error=%v", fixture.Valid, err)
			}
			if !fixture.Valid {
				return
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var actual any
			if err = json.Unmarshal(encoded, &actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, fixture.Expected) {
				expected, _ := json.Marshal(fixture.Expected)
				t.Fatalf("expected %s\nactual   %s", expected, encoded)
			}
		})
	}
}
