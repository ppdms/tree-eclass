package settings

import "strings"

// ProviderKeys maps each known provider to the credential names it accepts,
// in preference order. Register a new provider here (and its secret names in
// the workflow allowlist) to add it without touching callers: ProviderKey,
// ProviderAvailable, and validation all read this table.
var ProviderKeys = map[string][]string{
	"synthetic":   {"SYNTHETIC_API_KEY"},
	"ollama":      {"OLLAMA_API_KEY"},
	"huggingface": {"HF_TOKEN"},
	"alibaba":     {"ALIBABA_API_KEY", "DASHSCOPE_API_KEY"},
	"zai":         {"ZAI_API_KEY"},
	"opencode-go": {"OPENCODE_GO_API_KEY"},
	// Future providers: add "provider-id": {"PROVIDER_API_KEY"} here plus the
	// secret names in workflow.providerSecrets. Aliases (old vendor names)
	// append after the canonical name.
}

// ProviderKey selects the provider's configured key without copying unrelated
// runtime configuration.
func ProviderKey(keys map[string]string, provider string) string {
	for _, name := range ProviderKeys[provider] {
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
