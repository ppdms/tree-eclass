package messages

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestExportStream(t *testing.T) {
	for _, input := range []string{
		`{"messages":[{"id":"9007199254740993","content":"a}\\\"b"}],"channel":{"id":"12"}}`,
		`{"channel":{"id":"12"},"messages":[{"id":9007199254740993}]}`,
	} {
		stream := newExportStream(strings.NewReader(input))
		raw, err := stream.next()
		if err != nil {
			t.Fatal(err)
		}
		var value struct{ ID snowflake }
		if err = json.Unmarshal(raw, &value); err != nil || value.ID != 9007199254740993 {
			t.Fatal(string(raw), value, err)
		}
		if _, err = stream.next(); err != io.EOF {
			t.Fatal(err)
		}
		if len(stream.fields["channel"]) == 0 {
			t.Fatal("metadata after messages lost")
		}
	}
	for _, input := range []string{
		`{"messages":[]`,
		`{}`,
		`{"messages":null}`,
		`{"messages":[{},]}`,
		`{"messages":[42]}`,
		`{"messages":[],}`,
		`{"messages":[],"messages":[]}`,
		`{"messages":[]} true`,
		`{"messages":[{"v":"` + strings.Repeat("a", maxExportValue) + `"}]}`,
		`{"messages":[{"v":` + strings.Repeat("[", 65) + `0` + strings.Repeat("]", 65) + `}]}`,
	} {
		stream := newExportStream(strings.NewReader(input))
		var err error
		for err == nil {
			_, err = stream.next()
		}
		if err == io.EOF {
			t.Fatalf("accepted invalid input %.80s", input)
		}
	}
}
func TestNormalizeDiscord(t *testing.T) {
	raw := `{"id":"9007199254740993","timestamp":"2026-09-12T14:00:00+03:00","author":{"id":"7","nickname":"Νίκος"},"content":"εξέταση\u0000","reference":{"messageId":"9007199254740992"},"attachments":[{"id":"8","fileName":"ύλη.pdf","fileSizeBytes":9,"url":"https://example.test/a","filePath":"/private/secret"}],"embeds":[{"title":"Ανακοίνωση","fields":[{"name":"θέμα","value":"αλγόριθμοι"}]}],"reactions":[{"count":2},{"count":-1}],"isPinned":true}`
	m, err := normalizeMessage([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 9007199254740993 || m.Reply == nil || *m.Reply != 9007199254740992 ||
		m.Timestamp != "2026-09-12T11:00:00Z" ||
		m.Reactions != 2 ||
		m.Pinned != 1 {
		t.Fatalf("incorrect normalization: %+v", m)
	}
	if !strings.Contains(m.Content, "ύλη.pdf") || !strings.Contains(m.Search, "αλγοριθμοι") ||
		strings.Contains(m.Attachments, "secret") {
		t.Fatal("text or attachment normalization", m)
	}
	for _, raw := range []string{`{"id":1,"timestamp":"bad"}`, `{"id":-1}`, `{"id":"9223372036854775808"}`, `{"timestamp":"2026-09-12T11:00:00Z"}`} {
		if _, err = normalizeMessage([]byte(raw)); err == nil {
			t.Fatal("invalid message accepted", raw)
		}
	}
	if informative("https://tenor.com/a") || informative("pinned a message.") || informative("!!!") ||
		!informative("Ύλη εξέτασης") {
		t.Fatal("informative message selection")
	}
}
