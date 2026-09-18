// Package notifications delivers durable, bounded checker webhook messages.
package notifications

import (
	"errors"
	"strings"
	"unicode/utf16"
)

type Event struct {
	Key, Header string
	Lines       []string
	Error       bool
}

func width(text string) int {
	n := 0
	for _, r := range text {
		n += utf16.RuneLen(r)
	}
	return n
}
func clip(text string, limit int) string {
	if width(text) <= limit {
		return text
	}
	count := 0
	for index, r := range text {
		count += utf16.RuneLen(r)
		if count > limit-1 {
			return text[:index] + "…"
		}
	}
	return text
}

// Plain makes remote names safe inside our own Markdown headings and bullets.
func Plain(text string) string {
	return strings.NewReplacer("\\", "\\\\", "*", "\\*", "_", "\\_", "`", "\\`", "[", "\\[", "]", "\\]", "<", "‹", ">", "›", "\x00", "�").
		Replace(strings.Join(strings.Fields(text), " "))
}
func Batch(header string, lines []string) ([]string, error) {
	if len(lines) > 10000 {
		return nil, errors.New("notification exceeds 10000 items")
	}
	header = clip(header, 500)
	messages := []string{}
	current := header
	for _, line := range lines {
		line = clip(line, 1450)
		if line == "" {
			continue
		}
		separator := "\n\n"
		if current == "" {
			separator = ""
		}
		if width(current)+width(separator)+width(line) > 1950 {
			messages = append(messages, current)
			current = line
		} else {
			current += separator + line
		}
	}
	if current != "" {
		messages = append(messages, current)
	}
	return messages, nil
}
