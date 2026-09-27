package quota

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"tree-eclass/internal/domain/inference"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestQuotaProbeCredentialScopeAndResponseBounds(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.String() != "https://ollama.com/settings" || req.Header.Get("Cookie") != "session=fixture" ||
			req.Header.Get("Authorization") != "" {
			t.Error("quota credential scope", req.URL)
		}
		return &http.Response{
			StatusCode: 302,
			Header:     http.Header{"Location": []string{"https://attacker.invalid/"}},
			Body:       io.NopCloser(strings.NewReader("private echoed cookie")),
			Request:    req,
		}, nil
	})}
	probe := HTTPProbe{Client: client, OllamaCookie: "session=fixture"}
	if _, err := probe.Fetch(t.Context(), inference.Candidate{Provider: "ollama", APIKey: "different-fixture-key"}); err == nil ||
		strings.Contains(err.Error(), "echoed") ||
		calls != 1 {
		t.Fatal("redirect or secret error body", err, calls)
	}
	client.Transport = roundTrip(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != "https://api.synthetic.new/v2/quotas" ||
			req.Header.Get("Authorization") != "Bearer fixture" ||
			req.Header.Get("Cookie") != "" {
			t.Error("synthetic credential scope")
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(strings.Repeat(" ", 1000001))),
			Request:    req,
		}, nil
	})
	if _, err := probe.Fetch(t.Context(), inference.Candidate{Provider: "synthetic", APIKey: "fixture"}); err == nil {
		t.Fatal("oversized quota response accepted")
	}
}
