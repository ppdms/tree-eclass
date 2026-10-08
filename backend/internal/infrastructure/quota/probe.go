package quota

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"tree-eclass/internal/domain/inference"
)

type Probe interface {
	Fetch(context.Context, inference.Candidate) (QuotaSnapshot, error)
}
type HTTPProbe struct {
	Client       *http.Client
	OllamaCookie string
}

func (p HTTPProbe) Fetch(ctx context.Context, c inference.Candidate) (QuotaSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	endpoint := "https://api.synthetic.new/v2/quotas"
	if c.Provider == "ollama" {
		endpoint = "https://ollama.com/settings"
	} else if c.Provider != "synthetic" {
		return QuotaSnapshot{}, errors.New("provider has no quota probe")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if c.Provider == "ollama" {
		cookie, err := normalizeCookie(p.OllamaCookie)
		if err != nil {
			return QuotaSnapshot{}, err
		}
		req.Header.Set("Cookie", cookie)
		req.Header.Set("Accept", "text/html")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	} else {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("Accept", "application/json")
	}
	req.Header.Set("User-Agent", "tree-eclass/1.0")
	// Copy caller settings, but never permit cookie or credential redirects.
	client := *p.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return QuotaSnapshot{}, errors.New("provider quota endpoint is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return QuotaSnapshot{}, &inference.Error{
			Provider:   c.Provider,
			Status:     response.StatusCode,
			RetryAfter: inference.RetryAfter(response.Header.Get("Retry-After")),
			Reason:     "provider quota endpoint rejected the check",
		}
	}
	limit := int64(1000000)
	if c.Provider == "ollama" {
		limit = 5000000
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return QuotaSnapshot{}, errors.New("provider quota response is incomplete or too large")
	}
	if c.Provider == "ollama" {
		return parseOllama(raw)
	}
	return parseSynthetic(raw)
}
