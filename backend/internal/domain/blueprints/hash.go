package blueprints

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// PayloadHash hashes normalized native JSON, preserving numeric tokens from the
// stored source. Producers and freshness validation must use this same boundary.
func PayloadHash(raw []byte) (string, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	data := literalSeparators(bytes.TrimSuffix(buffer.Bytes(), []byte("\n")))
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
