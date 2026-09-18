package messages

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
)

const maxExportValue = 512 * 1024

// exportStream bounds individual JSON values while streaming the messages array.
// Metadata may occur on either side of the array; duplicate fields are rejected.
type exportStream struct {
	input                       *bufio.Reader
	fields                      map[string]json.RawMessage
	seen                        map[string]bool
	started, array, done, first bool
}

func newExportStream(input io.Reader) *exportStream {
	return &exportStream{
		input:  bufio.NewReaderSize(input, 32768),
		fields: map[string]json.RawMessage{},
		seen:   map[string]bool{},
		first:  true,
	}
}
func (s *exportStream) byte() (byte, error) {
	for {
		b, err := s.input.ReadByte()
		if err != nil {
			return b, err
		}
		if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
			return b, nil
		}
	}
}
func (s *exportStream) expect(want byte) error {
	b, err := s.byte()
	if err != nil {
		return err
	}
	if b != want {
		return errors.New("invalid Discord export JSON structure")
	}
	return nil
}
func (s *exportStream) value() (json.RawMessage, error) {
	b, err := s.byte()
	if err != nil {
		return nil, err
	}
	out := []byte{b}
	depth := 0
	if b == '{' || b == '[' {
		depth = 1
	}
	raw, err := s.scanValue(out, depth, b == '"')
	if err != nil {
		return nil, err
	}
	if !json.Valid(raw) {
		return nil, errors.New("invalid Discord export JSON value")
	}
	return raw, nil
}

func (s *exportStream) scanValue(out []byte, depth int, quoted bool) (json.RawMessage, error) {
	primitive := !quoted && depth == 0
	escaped := false
	for {
		if !primitive && !quoted && depth == 0 {
			break
		}
		b, err := s.input.ReadByte()
		if err == io.EOF && primitive {
			break
		}
		if err != nil {
			return nil, err
		}
		if primitive && (b == ',' || b == '}' || b == ']' || b == ' ' || b == '\n' || b == '\r' || b == '\t') {
			_ = s.input.UnreadByte()
			break
		}
		if len(out) >= maxExportValue {
			return nil, errors.New("Discord export value exceeds 512 KiB")
		}
		out = append(out, b)
		if quoted {
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		if b == '"' {
			quoted = true
		} else if b == '{' || b == '[' {
			depth++
			if depth > 64 {
				return nil, errors.New("Discord export nesting exceeds 64")
			}
		} else if b == '}' || b == ']' {
			depth--
		}
	}
	return out, nil
}
func (s *exportStream) next() (value json.RawMessage, err error) {
	defer func() {
		if err == io.EOF && !s.done {
			err = io.ErrUnexpectedEOF
		}
	}()
	if s.done {
		return nil, io.EOF
	}
	if !s.started {
		if err := s.expect('{'); err != nil {
			return nil, err
		}
		s.started = true
	}
	for {
		if s.array {
			returned, raw, err := s.nextArrayElement()
			if err != nil || returned {
				return raw, err
			}
		}
		key, err := s.nextFieldKey()
		if err != nil {
			return nil, err
		}
		if key == "messages" {
			if err = s.expect('['); err != nil {
				return nil, err
			}
			s.array = true
			s.first = true
			continue
		}
		raw, err := s.value()
		if err != nil {
			return nil, err
		}
		switch key {
		case "guild", "channel", "exportedAt", "messageCount":
			s.fields[key] = raw
		}
		s.first = false
	}
}

func (s *exportStream) nextArrayElement() (bool, json.RawMessage, error) {
	b, err := s.byte()
	if err != nil {
		return false, nil, err
	}
	if b == ']' {
		s.array = false
		s.first = false
		return false, nil, nil
	}
	if !s.first {
		if b != ',' {
			return true, nil, errors.New("invalid messages array separator")
		}
	} else {
		_ = s.input.UnreadByte()
	}
	s.first = false
	raw, err := s.value()
	if err == nil && (len(raw) == 0 || raw[0] != '{') {
		err = errors.New("Discord message must be an object")
	}
	return true, raw, err
}

func (s *exportStream) nextFieldKey() (string, error) {
	b, err := s.byte()
	if err != nil {
		return "", err
	}
	if b == '}' {
		if !s.seen["messages"] {
			return "", errors.New("Discord export has no messages array")
		}
		if _, err = s.byte(); err != io.EOF {
			return "", errors.New("trailing Discord export data")
		}
		s.done = true
		return "", io.EOF
	}
	if !s.first {
		if b != ',' {
			return "", errors.New("invalid export field separator")
		}
	} else {
		_ = s.input.UnreadByte()
	}
	raw, err := s.value()
	if err != nil {
		return "", err
	}
	var key string
	if err = json.Unmarshal(raw, &key); err != nil {
		return "", err
	}
	if s.seen[key] {
		return "", errors.New("duplicate Discord export field")
	}
	s.seen[key] = true
	if err = s.expect(':'); err != nil {
		return "", err
	}
	return key, nil
}
