package identity

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestJSONStorageTextRoundTrip(t *testing.T) {
	value := map[string]any{
		"key\x00\ue000": []any{"\ue0000", "\ue000e", "Greek Δένδρα\x00", map[string]any{"title": "\ue000\x00"}},
	}
	encoded := EncodeJSON(value)
	raw, err := json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var stored any
	if err = json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if got := DecodeJSON(stored); !reflect.DeepEqual(got, value) {
		t.Fatalf("text collision: %#v", got)
	}
}
