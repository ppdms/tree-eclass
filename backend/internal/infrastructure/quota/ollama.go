package quota

import (
	"errors"
	"regexp"
	"strings"
)

// The settings-page parsing contract derives from the existing MIT-attributed
// connector; preserve its attribution in THIRD_PARTY_NOTICES.md.
var usedPercent = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*%\s*used`)
var widthPercent = regexp.MustCompile(`(?i)width:\s*([0-9]+(?:\.[0-9]+)?)%`)
var resetTime = regexp.MustCompile(`data-time=["']([^"']+)`)
var usageLabels = []string{"Session usage", "Hourly usage", "Weekly usage"}

func normalizeCookie(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(value), "cookie:") {
		value = strings.TrimSpace(value[7:])
	}
	if value == "" || len(value) > 65536 || strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("Ollama quota cookie is missing or invalid")
	}
	known := map[string]bool{
		"session":                          true,
		"__Secure-session":                 true,
		"ollama_session":                   true,
		"__Host-ollama_session":            true,
		"wos-session":                      true,
		"__Secure-next-auth.session-token": true,
		"next-auth.session-token":          true,
	}
	for _, part := range strings.Split(value, ";") {
		name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok &&
			(known[name] || strings.HasPrefix(name, "__Secure-next-auth.session-token.") || strings.HasPrefix(name, "next-auth.session-token.")) {
			return value, nil
		}
	}
	return "", errors.New("Ollama quota cookie has no recognized session")
}
func usageWindow(html, label, name string, limit float64) *QuotaWindow {
	start := strings.Index(html, label)
	if start < 0 {
		return nil
	}
	tail := html[start+len(label):]
	end := min(4000, len(tail))
	for _, other := range usageLabels {
		if other == label {
			continue
		}
		if i := strings.Index(tail, other); i >= 0 {
			end = min(end, i)
		}
	}
	window := tail[:end]
	match := usedPercent.FindStringSubmatch(window)
	if match == nil {
		match = widthPercent.FindStringSubmatch(window)
	}
	if match == nil {
		return nil
	}
	used := numeric(match[1])
	if used == nil {
		return nil
	}
	*used = min(100, max(0, *used))
	result := &QuotaWindow{Name: name, Used: used, Limit: limit}
	if reset := resetTime.FindStringSubmatch(window); reset != nil {
		result.Reset = date(reset[1])
	}
	return result
}
func parseOllama(raw []byte) (QuotaSnapshot, error) {
	html := string(raw)
	result := QuotaSnapshot{Windows: []QuotaWindow{}}
	session := usageWindow(html, "Session usage", "session", 95)
	if session == nil {
		session = usageWindow(html, "Hourly usage", "session", 95)
	}
	if session != nil {
		result.Windows = append(result.Windows, *session)
	}
	if weekly := usageWindow(html, "Weekly usage", "weekly", 100); weekly != nil {
		result.Windows = append(result.Windows, *weekly)
	}
	if len(result.Windows) == 0 {
		return result, errors.New("Ollama usage is unavailable; check its quota cookie")
	}
	return result, nil
}
