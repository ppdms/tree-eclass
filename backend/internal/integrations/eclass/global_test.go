package eclass

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPublicFeedsAndDriveNeverInheritEclassCookies(t *testing.T) {
	c, _ := New(BaseURL, "private-student", "private-password")
	c.http.Jar.SetCookies(c.base, []*http.Cookie{{Name: "PHPSESSID", Value: "private-session", Path: "/"}})
	requests := 0
	c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || r.Method != "GET" {
			t.Fatal("credentials reached public request")
		}
		body := `<rss><channel><item><title>Announcement</title><link>https://aueb.gr/example</link></item></channel></rss>`
		media := "application/rss+xml"
		if r.URL.Path == "/modules/announcements/index.php" {
			media = "text/html"
			body = `<a class="tiny-icon-rss" href="/modules/announcements/rss.php?token=synthetic">RSS</a>`
		}
		if r.URL.Host == "drive.usercontent.google.com" {
			media = "application/pdf"
			body = "synthetic PDF"
		}
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": {media}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})
	for _, key := range []string{"dept", "undergrad", "rector"} {
		items, err := c.GlobalAnnouncements(t.Context(), key)
		if err != nil || len(items) != 1 {
			t.Fatal(key, items, err)
		}
	}
	d, err := c.Drive(t.Context(), "https://drive.google.com/file/d/abc_123/view", "notes.pdf")
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Body.Close()
	if requests != 5 {
		t.Fatal("unexpected requests", requests)
	}
	if _, err = c.GlobalAnnouncements(t.Context(), "unknown"); err == nil {
		t.Fatal("unknown public feed accepted")
	}
}

func TestDeadDriveFileKeepsItsLinkAsRedirect(t *testing.T) {
	c, _ := New(BaseURL, "private-student", "private-password")
	c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		status := http.StatusNotFound
		if strings.Contains(r.URL.RawQuery, "gone") {
			status = http.StatusGone
		}
		if strings.Contains(r.URL.RawQuery, "denied") {
			status = http.StatusForbidden
		}
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": {"text/html; charset=utf-8"}},
			Body:       io.NopCloser(strings.NewReader("<title>Error</title>")),
			Request:    r,
		}, nil
	})
	for _, id := range []string{"missing_1", "gone_2", "denied_3"} {
		raw := "https://drive.google.com/file/d/" + id + "/view"
		d, err := c.Drive(t.Context(), raw, "notes.pdf")
		if err != nil || d.Body != nil || d.Redirect != raw {
			t.Fatal(id, d, err)
		}
	}
}
