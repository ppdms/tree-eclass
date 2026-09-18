package settings

import "strings"

// ProviderKey selects aliases without copying unrelated runtime configuration.
func ProviderKey(keys map[string]string, provider string) string {
	names := map[string][]string{
		"synthetic":   {"SYNTHETIC_API_KEY"},
		"ollama":      {"OLLAMA_API_KEY"},
		"huggingface": {"HF_TOKEN"},
		"alibaba":     {"ALIBABA_API_KEY", "DASHSCOPE_API_KEY"},
		"zai":         {"ZAI_API_KEY"},
		"opencode-go": {"OPENCODE_GO_API_KEY"},
	}
	for _, name := range names[provider] {
		if value := strings.TrimSpace(keys[name]); value != "" {
			return value
		}
	}
	return ""
}
func (a AI) ProviderAvailable(keys map[string]string) bool {
	for _, provider := range a.ChatOrder {
		if a.Enabled(provider) && ProviderKey(keys, provider) != "" {
			return true
		}
	}
	return false
}
