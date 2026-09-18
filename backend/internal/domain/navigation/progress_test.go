package navigation

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestProgressMatchesNavigationFixture(t *testing.T) {
	// Fixtures exercise the navigation contract without opening a database.
	raw, err := os.ReadFile("testdata/progress.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Roadmap  bool            `json:"roadmap"`
		Include  bool            `json:"include_actions"`
		Content  map[string]any  `json:"content"`
		Units    json.RawMessage `json:"units"`
		Next     json.RawMessage `json:"next"`
		Actions  json.RawMessage `json:"actions"`
		Expected map[string]any  `json:"expected"`
	}
	if err = decodeJSON(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for i, fixture := range fixtures {
		if bytes.Equal(fixture.Next, []byte("null")) {
			fixture.Next = nil
		}
		if err = decorateProgress(
			fixture.Content,
			fixture.Units,
			fixture.Next,
			fixture.Actions,
			Request{Roadmap: fixture.Roadmap, IncludeActions: fixture.Include},
		); err != nil {
			t.Fatal(err)
		}
		actual, _ := json.Marshal(fixture.Content)
		expected, _ := json.Marshal(fixture.Expected)
		if !bytes.Equal(actual, expected) {
			t.Fatalf("fixture %d\nexpected %s\nactual   %s", i, expected, actual)
		}
	}
}
