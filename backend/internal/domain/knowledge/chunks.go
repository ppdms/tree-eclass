// Package knowledge owns locator-preserving indexing and evidence retrieval.
package knowledge

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
	"tree-eclass/internal/domain/extract"
	"tree-eclass/internal/domain/identity"
)

var horizontal = regexp.MustCompile(`[ \t]+`)
var paragraphs = regexp.MustCompile(`\n\s*\n`)

type Chunk struct {
	ID             string         `json:"id"`
	DocumentID     string         `json:"document_id"`
	Ordinal        int64          `json:"ordinal"`
	LocatorType    string         `json:"locator_type"`
	LocatorStart   string         `json:"locator_start"`
	LocatorEnd     *string        `json:"locator_end"`
	Heading        *string        `json:"heading"`
	Text           string         `json:"text"`
	NormalizedText string         `json:"normalized_text"`
	ContentHash    string         `json:"content_hash"`
	Metadata       map[string]any `json:"metadata"`
}

func Pieces(text string, target, overlap int) []string {
	text = strings.TrimSpace(horizontal.ReplaceAllString(norm.NFC.String(text), " "))
	if text == "" {
		return nil
	}
	if len([]rune(text)) <= target {
		return []string{text}
	}
	result := []string{}
	current := ""
	for _, paragraph := range paragraphs.Split(text, -1) {
		paragraph = strings.TrimSpace(paragraph)
		for len([]rune(paragraph)) > target {
			if current != "" {
				result = append(result, current)
				current = ""
			}
			runes := []rune(paragraph)
			cut := target
			for i := target - 1; i > target/2; i-- {
				if runes[i] == ' ' {
					cut = i
					break
				}
			}
			result = append(result, strings.TrimSpace(string(runes[:cut])))
			paragraph = strings.TrimSpace(string(runes[max(0, cut-overlap):]))
		}
		candidate := paragraph
		if current != "" {
			candidate = strings.TrimSpace(current + "\n\n" + paragraph)
		}
		if len([]rune(candidate)) > target && current != "" {
			result = append(result, current)
			tail := ""
			runes := []rune(current)
			if overlap > 0 {
				tail = string(runes[max(0, len(runes)-overlap):])
			}
			current = strings.TrimSpace(tail + "\n\n" + paragraph)
		} else {
			current = candidate
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
func Chunks(document, hash string, unit extract.Record, ordinal int64) []Chunk {
	end := ""
	if unit.LocatorEnd != nil {
		end = *unit.LocatorEnd
	}
	chunks := []Chunk{}
	for _, text := range Pieces(unit.Text, 4000, 300) {
		id := identity.Stable("chk", document, hash, unit.LocatorType, unit.LocatorStart, end, fmt.Sprint(ordinal))
		chunks = append(
			chunks,
			Chunk{
				id,
				document,
				ordinal,
				unit.LocatorType,
				unit.LocatorStart,
				unit.LocatorEnd,
				unit.Heading,
				text,
				identity.Search(text),
				identity.TextHash(text),
				unit.Metadata,
			},
		)
		ordinal++
	}
	return chunks
}
