package inference

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

func ParseObject(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "```") {
		_, raw, _ = strings.Cut(raw, "\n")
		raw = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), "```"))
	}
	if len(raw) > 1024*1024 || !json.Valid([]byte(raw)) {
		return nil, errors.New("model response is not one bounded JSON object")
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	if decoder.Decode(&result) != nil || result == nil {
		return nil, errors.New("model response is not an object")
	}
	return result, nil
}
