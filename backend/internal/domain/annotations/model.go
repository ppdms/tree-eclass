// Package annotations preserves learner evidence across source revisions.
package annotations

import (
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"

	"tree-eclass/internal/domain/identity"
)

type Annotation struct {
	ID           int64             `json:"id"`
	CourseID     int64             `json:"course_id"`
	DocumentID   string            `json:"document_id"`
	SourceHash   string            `json:"source_hash"`
	PageNumber   int64             `json:"page_number"`
	Kind         string            `json:"kind"`
	Origin       string            `json:"origin"`
	Status       string            `json:"status"`
	Color        string            `json:"color"`
	Quote        string            `json:"quote"`
	Prefix       string            `json:"prefix"`
	Suffix       string            `json:"suffix"`
	CharStart    *int64            `json:"char_start"`
	CharEnd      *int64            `json:"char_end"`
	Rects        []json.RawMessage `json:"rects"`
	ChunkID      *string           `json:"chunk_id"`
	Body         *string           `json:"body"`
	Tags         []string          `json:"tags"`
	ActionID     string            `json:"action_id"`
	UnitKey      string            `json:"unit_key"`
	PlanRevision string            `json:"plan_revision"`
	SessionID    *int64            `json:"session_id"`
	CreatedAt    string            `json:"created_at"`
	UpdatedAt    string            `json:"updated_at"`
}

type Create struct {
	Annotation
	IdempotencyKey string `json:"idempotency_key"`
}
type Update struct {
	Body   *string   `json:"body"`
	Color  *string   `json:"color"`
	Tags   *[]string `json:"tags"`
	Status *string   `json:"status"`
}

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

func clip(s string, n int) string { return string([]rune(s)[:min(len([]rune(s)), n)]) }
func (c *Create) Validate() error {
	if c.CourseID < 1 || c.CourseID > 1<<31 || c.PageNumber < 1 || c.PageNumber > 100000 {
		return errors.New("course_id and page_number must be positive bounded integers")
	}
	c.DocumentID = strings.TrimSpace(c.DocumentID)
	if c.DocumentID == "" || len(c.DocumentID) > 200 {
		return errors.New("document_id is required and must fit 200 bytes")
	}
	c.IdempotencyKey = strings.TrimSpace(c.IdempotencyKey)
	if !keyPattern.MatchString(c.IdempotencyKey) {
		return errors.New(
			"idempotency_key must contain 8 to 128 letters, numbers, dots, underscores, colons, or hyphens",
		)
	}
	if c.Kind == "" {
		c.Kind = "highlight"
	}
	if !slices.Contains([]string{"highlight", "note", "question", "bookmark"}, c.Kind) {
		return errors.New("unsupported annotation kind")
	}
	c.Quote = clip(c.Quote, 4000)
	if c.Kind == "highlight" && strings.TrimSpace(c.Quote) == "" {
		return errors.New("a highlight requires the text it covers")
	}
	if c.CharStart != nil && *c.CharStart < 0 || c.CharEnd != nil && *c.CharEnd < 0 ||
		c.CharStart != nil && c.CharEnd != nil && *c.CharEnd < *c.CharStart {
		return errors.New("invalid character range")
	}
	if c.Color == "" {
		c.Color = "yellow"
	}
	c.Color, c.Prefix, c.Suffix = clip(c.Color, 32), clip(c.Prefix, 600), clip(c.Suffix, 600)
	c.ActionID, c.UnitKey, c.PlanRevision = clip(c.ActionID, 240), clip(c.UnitKey, 160), clip(c.PlanRevision, 128)
	c.Tags = normalizeTags(c.Tags)
	if c.Rects == nil {
		c.Rects = []json.RawMessage{}
	}
	encoded, err := json.Marshal(c.Rects)
	if err != nil || len(encoded) > 60000 {
		return errors.New("annotation rectangles exceed 60000 bytes")
	}
	if c.Body != nil {
		text := clip(strings.TrimSpace(*c.Body), 20000)
		c.Body = &text
	}
	return nil
}
func normalizeTags(tags []string) []string {
	result := make([]string, 0, min(len(tags), 20))
	for _, tag := range tags[:min(len(tags), 20)] {
		result = append(result, clip(tag, 60))
	}
	return result
}
func (u Update) Validate() error {
	if u.Status != nil && !slices.Contains([]string{"active", "orphaned", "deleted"}, *u.Status) {
		return errors.New("unsupported annotation status")
	}
	return nil
}
func decode(data []byte) (Annotation, error) {
	var result Annotation
	err := json.Unmarshal(data, &result)
	for _, text := range []*string{
		&result.Quote,
		&result.Prefix,
		&result.Suffix,
		&result.Color,
		&result.ActionID,
		&result.UnitKey,
		&result.PlanRevision,
		result.Body,
	} {
		if text != nil {
			*text = identity.Decode(*text)
		}
	}
	for i := range result.Tags {
		result.Tags[i] = identity.Decode(result.Tags[i])
	}
	return result, err
}
