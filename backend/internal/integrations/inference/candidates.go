package inference

import (
	"slices"
	"strings"

	"tree-eclass/internal/domain/settings"
)

// Endpoints are application configuration, never supplied by a model or tool.
var endpoints = map[string]string{
	"synthetic":   "https://api.synthetic.new/openai/v1/chat/completions",
	"ollama":      "https://ollama.com/api/chat",
	"huggingface": "https://router.huggingface.co/v1/chat/completions",
	"alibaba":     "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
	"zai":         "https://api.z.ai/api/coding/paas/v4/chat/completions",
	"opencode-go": "https://opencode.ai/zen/go/v1/chat/completions",
}

// ChatCandidates preserves provider priority. A picker model changes the model
// only on providers that can serve it; fallbacks retain their configured model.
func ChatCandidates(a settings.AI, keys map[string]string, requested string) []Candidate {
	if !slices.Contains(a.ChatModels, requested) {
		requested = a.DefaultModel
	}
	result := []Candidate{}
	for _, name := range a.ChatOrder {
		key := settings.ProviderKey(keys, name)
		if !a.Enabled(name) || key == "" || endpoints[name] == "" {
			continue
		}
		model := a.ChatModel(name)
		if supportsChat(name, requested, model) {
			model = requested
		}
		if !supportsChat(name, model, a.ChatModel(name)) {
			continue
		}
		result = append(result, candidate(name, model, key, &a.Think))
	}
	return result
}

func candidate(name, model, key string, think *bool) Candidate {
	if name == "huggingface" {
		model = strings.TrimPrefix(model, "hf:")
	}
	if name == "alibaba" {
		model = strings.TrimPrefix(model, "ali:")
	}
	if name == "zai" {
		model = strings.TrimPrefix(model, "zai:")
	}
	if name == "synthetic" || name == "huggingface" {
		think = nil
	}
	return Candidate{Provider: name, Model: model, Endpoint: endpoints[name], APIKey: key, Think: think}
}

func supportsChat(provider, model, configured string) bool {
	if model == "" {
		return false
	}
	// An explicit provider-specific model setting is authoritative. Shared
	// unprefixed models retain the legacy cross-provider picker behavior.
	if model == configured {
		return true
	}
	switch provider {
	case "synthetic":
		return strings.HasPrefix(model, "syn:")
	case "huggingface":
		return strings.Contains(strings.TrimPrefix(model, "hf:"), "/") && !strings.HasPrefix(model, "syn:")
	case "alibaba":
		return strings.HasPrefix(model, "ali:") || model == "qwen3.8-flash" || model == "deepseek-v4-flash-0731"
	case "ollama":
		return slices.Contains([]string{"glm-5.2", "minimax-m3", "kimi-k2.6", "qwen3.5:397b"}, model)
	case "opencode-go":
		return slices.Contains(
			[]string{
				"minimax-m3",
				"minimax-m2.7",
				"minimax-m2.5",
				"kimi-k3",
				"kimi-k2.7-code",
				"kimi-k2.6",
				"kimi-k2.5",
				"glm-5.2",
				"glm-5.1",
				"glm-5",
				"deepseek-v4-pro",
				"deepseek-v4-flash",
				"qwen3.7-max",
				"qwen3.7-plus",
				"qwen3.6-plus",
				"qwen3.5-plus",
				"mimo-v2-pro",
				"mimo-v2-omni",
				"mimo-v2.5-pro",
				"mimo-v2.5",
				"hy3",
				"hy3-preview",
				"gpt-5.6-luna",
				"grok-4.5",
			},
			model,
		)
	}
	return false
}
