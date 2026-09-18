package settings

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestAIRoutingFixture(t *testing.T) {
	var fixtures []struct {
		Input      AI
		Valid      bool
		Normalized AI
	}
	data, err := os.ReadFile("testdata/ai-routing.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for i, fixture := range fixtures {
		got := fixture.Input
		err = got.Normalize()
		if (err == nil) != fixture.Valid {
			t.Errorf("fixture %d: validation mismatch: %v", i, err)
			continue
		}
		if fixture.Valid && !reflect.DeepEqual(got, fixture.Normalized) {
			t.Errorf("fixture %d: provider/model fallback differs from fixture", i)
		}
	}
}
