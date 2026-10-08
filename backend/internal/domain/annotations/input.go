package annotations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Browser route parameters are strings. Accept whole decimal strings as the
// previous API did, while rejecting booleans and fractional JSON numbers.
func (c *Create) UnmarshalJSON(data []byte) error {
	type alias Create
	input := struct {
		*alias
		Course  json.RawMessage `json:"course_id"`
		Page    json.RawMessage `json:"page_number"`
		Start   json.RawMessage `json:"char_start"`
		End     json.RawMessage `json:"char_end"`
		Session json.RawMessage `json:"session_id"`
	}{alias: (*alias)(c)}
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	for _, field := range []struct {
		raw    json.RawMessage
		name   string
		target **int64
	}{
		{input.Start, "char_start", &c.CharStart}, {input.End, "char_end", &c.CharEnd},
		{input.Session, "session_id", &c.SessionID},
	} {
		if len(field.raw) == 0 || bytes.Equal(field.raw, []byte("null")) {
			continue
		}
		value, err := integer(field.raw, field.name)
		if err != nil {
			return err
		}
		*field.target = &value
	}
	var err error
	if c.CourseID, err = integer(input.Course, "course_id"); err != nil {
		return err
	}
	c.PageNumber, err = integer(input.Page, "page_number")
	return err
}

func integer(raw []byte, name string) (int64, error) {
	text := string(raw)
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole integer", name)
	}
	return value, nil
}
