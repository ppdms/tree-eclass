package navigation

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"tree-eclass/internal/domain/identity"
)

func safeURL(raw string, discord bool) any {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		strings.ContainsFunc(raw, unicode.IsControl) {
		return nil
	}
	if discord && u.Hostname() != "discord.com" && u.Hostname() != "discordapp.com" {
		return nil
	}
	return u.String()
}

func documentLink(d evidenceDocument) map[string]any {
	class := "official_material"
	if d.Origin == "external" {
		class = "course_material"
	}
	return map[string]any{"evidence_ref": "document:" + d.ID, "document_id": d.ID,
		"label": identity.Decode(d.Name), "title": identity.Decode(d.Name), "source_path": identity.Decode(d.Path),
		"source_kind": d.Origin, "source_origin": d.Origin, "evidence_class": class, "available": true,
		"document_kind": d.Kind, "renderable": d.Kind == "pdf" || d.Kind == "image",
		"resource_uri": "eclass://documents/" + url.PathEscape(
			d.ID,
		), "url": safeURL(d.URL, false), "untrusted_content": true}
}

func addCommunityLinks(links map[string]map[string]any, packet map[string]any) {
	items, _ := packet["evidence"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		ref, _ := item["evidence_ref"].(string)
		if !strings.HasPrefix(ref, "discord:") {
			continue
		}
		id := strings.TrimPrefix(ref, "discord:")
		channel, _ := item["channel_name"].(string)
		if channel == "" {
			channel = "Discord discussion"
		}
		var link any
		candidates := []any{item["message_url"]}
		if urls, ok := item["message_urls"].([]any); ok {
			candidates = append(candidates, urls...)
		}
		for _, raw := range candidates {
			if text, ok := raw.(string); ok {
				if link = safeURL(text, true); link != nil {
					break
				}
			}
		}
		ids := []string{}
		if rows, ok := item["message_ids"].([]any); ok {
			for _, value := range rows[:min(50, len(rows))] {
				ids = append(ids, fmt.Sprint(value))
			}
		}
		links[ref] = map[string]any{
			"evidence_ref":       ref,
			"conversation_id":    id,
			"label":              "#" + channel,
			"title":              "#" + channel,
			"source_kind":        "community_discussion",
			"source_origin":      "discord_messages",
			"evidence_class":     "community_discussion",
			"channel_name":       channel,
			"ended_at":           item["ended_at"],
			"message_ids":        ids,
			"url":                link,
			"resource_uri":       "discord://conversations/" + url.PathEscape(id),
			"available":          true,
			"community_reported": true,
			"untrusted_content":  true,
		}
	}
}
