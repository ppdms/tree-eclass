package workflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"tree-eclass/internal/domain/settings"
)

// providerSecrets is the credential allowlist for import and validation.
// Provider key names derive from the settings registry so a new provider
// needs only a registry entry; the trailing names are non-provider secrets
// (cookies, future-provider placeholders) sharing the same credential file.
var providerSecrets = providerSecretAllowlist()

func providerSecretAllowlist() []string {
	seen := map[string]bool{}
	var out []string
	for _, names := range settings.ProviderKeys {
		for _, name := range names {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	for _, name := range []string{
		"OLLAMA_COOKIE_HEADER",
		"OPENROUTER_API_KEY",
		"OPENAI_API_KEY",
		"KNOWLEDGE_EMBEDDING_API_KEY",
	} {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// Import only provider keys from surviving configuration. This is never used by
// application startup once the checkpointed credential file exists.
func (c *Controller) legacyProviderKeys(ctx context.Context) (map[string]string, error) {
	var raw []byte
	var err error
	if runtime.GOOS == "darwin" {
		raw, err = keychainProviders(ctx)
	} else {
		raw, err = os.ReadFile(filepath.Join(filepath.Dir(c.Root), "credentials.json"))
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	}
	if err != nil {
		return nil, err
	}
	values := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &values); err != nil {
			return nil, errors.New("stored provider configuration is not valid JSON")
		}
	}
	result := map[string]string{}
	for _, name := range providerSecrets {
		var value string
		if data := values[name]; len(data) > 0 {
			if err = json.Unmarshal(data, &value); err != nil {
				return nil, fmt.Errorf("stored provider key %s is not text", name)
			}
		}
		if override, exists := os.LookupEnv(name); exists {
			value = override
		}
		if value != "" {
			result[name] = value
		}
	}
	return result, nil
}
func keychainProviders(ctx context.Context) ([]byte, error) {
	account, err := user.Current()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(
		ctx,
		"security",
		"find-generic-password",
		"-s",
		"tree-eclass",
		"-a",
		account.Username,
		"-w",
	)
	raw, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 44 {
			return nil, nil
		}
		return nil, errors.New("could not read provider keys; unlock the macOS login Keychain and retry")
	}
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, "tree-v1:") {
		return base64.StdEncoding.DecodeString(strings.TrimPrefix(text, "tree-v1:"))
	}
	return []byte(text), nil
}
