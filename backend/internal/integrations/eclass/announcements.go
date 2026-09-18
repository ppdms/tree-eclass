package eclass

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

type Announcement struct {
	ID          string  `json:"announcement_id"`
	Title       string  `json:"title"`
	Link        string  `json:"link"`
	Description string  `json:"description"`
	Published   *string `json:"pub_date"`
}
type rssItem struct {
	GUID        string `xml:"guid"`
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	Date        string `xml:"pubDate"`
}

var guidDigits = regexp.MustCompile(`(\d+)$`)

func announcementID(guid, link string) string {
	// Preserve the existing GUID identity, including eClass's timezone prefix.
	parts := strings.Split(strings.TrimSpace(guid), " ")
	if len(parts) > 1 {
		if id := guidDigits.FindString(parts[len(parts)-1]); id != "" {
			return id
		}
	}
	u, err := url.Parse(link)
	if err == nil {
		if id := u.Query().Get("an_id"); id != "" {
			return id
		}
	}
	return link
}

func ParseRSS(data []byte) ([]Announcement, error) {
	var feed struct {
		XMLName xml.Name `xml:"rss"`
		Channel *struct {
			Items []rssItem `xml:"item"`
		} `xml:"channel"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(data), &feed); err != nil {
		return nil, errors.New("invalid announcement RSS")
	}
	if feed.Channel == nil {
		return nil, errors.New("announcement RSS has no channel")
	}
	out := make([]Announcement, 0, len(feed.Channel.Items))
	for _, item := range feed.Channel.Items {
		a := Announcement{
			ID:          announcementID(item.GUID, strings.TrimSpace(item.Link)),
			Title:       strings.TrimSpace(item.Title),
			Link:        strings.TrimSpace(item.Link),
			Description: strings.TrimSpace(item.Description),
		}
		if a.ID == "" {
			return nil, errors.New("announcement has no stable identity")
		}
		if a.Title == "" {
			a.Title = "Untitled"
		}
		if date, err := mail.ParseDate(strings.TrimSpace(item.Date)); err == nil {
			value := date.Format("2006-01-02T15:04:05-07:00")
			a.Published = &value
		}
		out = append(out, a)
	}
	return out, nil
}

func (c *Client) Announcements(ctx context.Context, courseID int64) ([]Announcement, error) {
	data, err := c.Page(ctx, fmt.Sprintf("/modules/announcements/index.php?course=INF%d", courseID))
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	a := first(doc, "a", "tiny-icon-rss")
	if a == nil {
		return nil, errors.New("announcement RSS link is missing")
	}
	raw, err := c.Resolve(attr(a, "href"))
	if err != nil {
		return nil, err
	}
	data, err = c.Page(ctx, raw)
	if err != nil {
		return nil, err
	}
	return ParseRSS(data)
}
