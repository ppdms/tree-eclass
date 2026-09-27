package scheduler

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestIndependentSchedules(t *testing.T) {
	raw, err := os.ReadFile("testdata/schedules.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name     string
		Input    Input
		Expected any
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			result, err := Build(t.Context(), fixture.Input)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(result)
			var actual any
			if err = json.Unmarshal(raw, &actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, fixture.Expected) {
				t.Fatalf("schedule mismatch\nactual: %s\nexpected: %#v", raw, fixture.Expected)
			}
		})
	}
}
