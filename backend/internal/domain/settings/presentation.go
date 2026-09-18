package settings

import (
	"context"
	"encoding/json"
	"strings"

	"tree-eclass/internal/domain/courses"
)

type ProviderStatus struct {
	Key    bool   `json:"key"`
	Status string `json:"status"`
}
type PublicAI struct {
	AI
	Credentials map[string]bool           `json:"provider_credentials"`
	Status      map[string]ProviderStatus `json:"provider_status"`
}
type Page struct {
	Courses        []courses.Course `json:"courses"`
	Hidden         []courses.Course `json:"hidden_courses"`
	Preferences    Preferences      `json:"preferences"`
	Discord        Discord          `json:"discord_exporter"`
	Channels       []DiscordChannel `json:"discord_channels"`
	Mapped         int              `json:"discord_mapped_count"`
	DiscordToken   bool             `json:"discord_export_token_configured"`
	HasCredentials bool             `json:"has_credentials"`
	Username       string           `json:"credential_username"`
	AI             PublicAI         `json:"ai_settings"`
}

func (s Service) PublicAI(ctx context.Context, keys map[string]string) (PublicAI, error) {
	a, err := s.AI(ctx)
	if err != nil {
		return PublicAI{}, err
	}
	result := PublicAI{AI: a, Credentials: map[string]bool{}, Status: map[string]ProviderStatus{}}
	for _, provider := range Providers {
		present := ProviderKey(keys, provider) != ""
		result.Credentials[provider] = present
		result.Status[provider] = ProviderStatus{present, "unknown"}
	}
	rows, err := s.Pool.Query(ctx, `SELECT key,value FROM knowledge.knowledge_state WHERE key LIKE '%\_quota'`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err = rows.Scan(&key, &value); err != nil {
			return result, err
		}
		provider := strings.TrimSuffix(key, "_quota")
		state, known := result.Status[provider]
		if !known {
			continue
		}
		var data struct {
			Status string `json:"status"`
		}
		if json.Unmarshal([]byte(value), &data) == nil && data.Status != "" {
			state.Status = data.Status
			result.Status[provider] = state
		}
	}
	return result, rows.Err()
}

func (s Service) Page(ctx context.Context, keys map[string]string) (Page, error) {
	p := Page{Hidden: []courses.Course{}}
	var err error
	if p.Courses, err = (courses.Service{Pool: s.Pool}).List(ctx, true); err != nil {
		return p, err
	}
	for _, c := range p.Courses {
		if c.Hidden {
			p.Hidden = append(p.Hidden, c)
		}
	}
	if p.Preferences, err = s.Preferences(ctx); err != nil {
		return p, err
	}
	if p.Discord, err = s.Discord(ctx); err != nil {
		return p, err
	}
	p.DiscordToken = p.Discord.Token != ""
	p.Discord.Token = ""
	if p.Channels, err = s.DiscordChannels(ctx); err != nil {
		return p, err
	}
	for _, c := range p.Channels {
		if c.CourseID != nil {
			p.Mapped++
		}
	}
	credentials, err := s.Credentials(ctx)
	if err != nil {
		return p, err
	}
	if credentials != nil {
		p.Username = credentials.Username
		p.HasCredentials = credentials.Username != "" && credentials.Password != ""
	}
	if p.AI, err = s.PublicAI(ctx, keys); err != nil {
		return p, err
	}
	return p, nil
}
