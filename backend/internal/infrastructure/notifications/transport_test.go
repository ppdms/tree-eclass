package notifications

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestUnicodeBatchAndMentions(t *testing.T) {
	messages, err := Batch(
		"**Ύλη**",
		[]string{strings.Repeat("😀", 2000), "@everyone αλγόριθμοι", strings.Repeat("β", 1900)},
	)
	if err != nil || len(messages) < 2 {
		t.Fatal(messages, err)
	}
	for _, m := range messages {
		if width(m) > 1950 {
			t.Fatal("Discord UTF-16 limit exceeded")
		}
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("wait") != "true" {
			t.Error("webhook must wait for acknowledgement")
		}
		var payload struct {
			Content  string
			Mentions struct{ Parse []string } `json:"allowed_mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Mentions.Parse) != 0 ||
			payload.Mentions.Parse == nil {
			t.Error("unexpected mentions", err)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"id":"123"}`))
	}))
	defer endpoint.Close()
	sender := NewSender()
	defer sender.Close()
	if _, err = sender.Send(t.Context(), endpoint.URL, "@everyone"); err != nil {
		t.Fatal(err)
	}
}
func TestRateLimitsAndRedirects(t *testing.T) {
	calls := 0
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/limited" {
			w.Header().Set("Retry-After", "1.25")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"retry_after":2.5}`))
			return
		}
		w.Header().Set("Location", "/secret-target")
		w.WriteHeader(302)
	}))
	defer endpoint.Close()
	sender := NewSender()
	defer sender.Close()
	delay, err := sender.Send(t.Context(), endpoint.URL+"/limited", "hello")
	var failure Failure
	if !errors.As(err, &failure) || failure.Status != 429 || delay != 2500*time.Millisecond {
		t.Fatal(delay, err)
	}
	_, err = sender.Send(t.Context(), endpoint.URL+"/redirect", "hello")
	if !errors.As(err, &failure) || failure.Status != 302 || calls != 2 {
		t.Fatal("redirect followed", calls, err)
	}
	for _, url := range []string{"file:///private/secret", "https://user:secret@example.test", "relative", "https://example.test#fragment"} {
		if err = ValidURL(url); err == nil {
			t.Fatal("unsafe webhook URL", url)
		}
	}
	if got := Plain("<script> @everyone **name**\x00\nnext"); strings.Contains(got, "<script>") ||
		strings.Contains(got, "\x00") {
		t.Fatal("source formatting", got)
	}
}
