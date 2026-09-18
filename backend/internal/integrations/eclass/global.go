package eclass

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"

	"golang.org/x/net/html"
)

var globalURLs = map[string]string{
	"undergrad": "https://aueb.gr/el/taxonomy/term/701/feed",
	"rector":    "https://aueb.gr/el/taxonomy/term/700/feed",
}

func (c *Client) publicFeedOrigin(u *url.URL) bool {
	if u.User != nil {
		return false
	}
	if c.sameOrigin(u) {
		return true
	}
	return u.Scheme == "https" && u.Port() == "" && (u.Hostname() == "aueb.gr" || u.Hostname() == "www.aueb.gr")
}

func (c *Client) publicPage(ctx context.Context, raw string) ([]byte, error) {
	u, err := url.Parse(raw)
	if err != nil || !c.publicFeedOrigin(u) {
		return nil, errors.New("invalid public announcement origin")
	}
	client := &http.Client{
		Timeout:   c.http.Timeout,
		Transport: c.http.Transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 || !c.publicFeedOrigin(req.URL) {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("public announcement request failed")
	}
	return readPage(resp)
}

func (c *Client) GlobalAnnouncements(ctx context.Context, key string) ([]Announcement, error) {
	raw, ok := globalURLs[key]
	if key == "dept" {
		page, err := c.Resolve("/modules/announcements/index.php?course=INF417")
		if err != nil {
			return nil, err
		}
		data, err := c.publicPage(ctx, page)
		if err != nil {
			return nil, err
		}
		doc, err := html.Parse(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		a := first(doc, "a", "tiny-icon-rss")
		if a == nil {
			return nil, errors.New("department RSS link is missing")
		}
		raw, err = c.Resolve(attr(a, "href"))
		if err != nil {
			return nil, err
		}
		ok = true
	}
	if !ok {
		return nil, errors.New("unknown global announcement feed")
	}
	data, err := c.publicPage(ctx, raw)
	if err != nil {
		return nil, err
	}
	return ParseRSS(data)
}
