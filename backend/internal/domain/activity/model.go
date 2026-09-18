// Package activity reads bounded event pages before loading their source bodies.
package activity

import (
	"encoding/json"
	"strings"

	"tree-eclass/internal/domain/identity"
)

type Change struct {
	Type     string  `json:"change_type"`
	Path     string  `json:"file_path"`
	Name     *string `json:"display_name"`
	Redirect *string `json:"redirect_url"`
	Diff     *string `json:"diff_webdav_path"`
}

type Item struct {
	Type        string          `json:"type"`
	ID          json.RawMessage `json:"id"`
	Timestamp   *string         `json:"timestamp"`
	SortKey     string          `json:"sort_key"`
	CourseID    *int64          `json:"course_id"`
	CourseName  string          `json:"course_name"`
	ShortName   *string         `json:"course_short_name"`
	ChangeNo    string          `json:"change_no,omitempty"`
	Message     *string         `json:"message,omitempty"`
	Changes     []Change        `json:"changes,omitempty"`
	Title       string          `json:"title,omitempty"`
	Link        string          `json:"link,omitempty"`
	Description *string         `json:"description,omitempty"`
}

func (item *Item) decode() {
	for _, value := range []*string{
		&item.CourseName,
		item.ShortName,
		&item.Title,
		&item.Link,
		item.Description,
		item.Message,
	} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	for i := range item.Changes {
		c := &item.Changes[i]
		for _, value := range []*string{&c.Path, c.Name, c.Redirect, c.Diff} {
			if value != nil {
				*value = identity.Decode(*value)
			}
		}
	}
}

func (item Item) key() string {
	if len(item.ID) > 0 && item.ID[0] == '"' {
		var text string
		if json.Unmarshal(item.ID, &text) == nil {
			return text
		}
	}
	return strings.TrimSpace(string(item.ID))
}

type Group struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Title      string `json:"title"`
	Importance string `json:"importance"`
	Link       string `json:"link"`
	Items      []Item `json:"items"`
}

type Page struct {
	Timeline *[]Item `json:"timeline,omitempty"`
	Groups   []Group `json:"groups"`
	More     bool    `json:"has_more"`
	Next     int64   `json:"next_offset"`
}
