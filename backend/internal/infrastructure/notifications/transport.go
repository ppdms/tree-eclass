package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"tree-eclass/internal/domain/identity"
)

type Sender interface {
	Send(context.Context, string, string) (time.Duration, error)
}
type HTTPSender struct{ Client *http.Client }
type Failure struct{ Status int }

func (e Failure) Error() string { return fmt.Sprintf("webhook returned HTTP %d", e.Status) }

// ValidURL is the shared webhook-target validator defined by domain/identity.
var ValidURL = identity.ValidURL

func NewSender() *HTTPSender {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1
	transport.MaxIdleConnsPerHost = 1
	transport.ResponseHeaderTimeout = 10 * time.Second
	return &HTTPSender{
		Client: &http.Client{
			Transport:     transport,
			Timeout:       15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}
func (s *HTTPSender) Close() { s.Client.CloseIdleConnections() }
func (s *HTTPSender) Send(ctx context.Context, target, content string) (time.Duration, error) {
	if err := ValidURL(target); err != nil {
		return 0, err
	}
	if width(content) > 2000 {
		return 0, errors.New("webhook message exceeds 2000 characters")
	}
	u, _ := url.Parse(target)
	query := u.Query()
	query.Set("wait", "true")
	u.RawQuery = query.Encode()
	body, err := json.Marshal(
		map[string]any{
			"username":         "tree-eclass",
			"content":          content,
			"allowed_mentions": map[string]any{"parse": []string{}},
		},
	)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("invalid webhook request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.Client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, errors.New("webhook request failed before acknowledgement")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(data) > 65536 {
		return 0, errors.New("webhook acknowledgement was incomplete or oversized")
	}
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		if response.Header.Get("X-RateLimit-Remaining") == "0" {
			return retryAfter(response.Header.Get("X-RateLimit-Reset-After"), nil), nil
		}
		return 0, nil
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return retryAfter(response.Header.Get("Retry-After"), data), Failure{http.StatusTooManyRequests}
	}
	return 0, Failure{response.StatusCode}
}
func retryAfter(header string, body []byte) time.Duration {
	seconds, _ := strconv.ParseFloat(header, 64)
	var data struct {
		Seconds float64 `json:"retry_after"`
	}
	if json.Unmarshal(body, &data) == nil && data.Seconds > seconds {
		seconds = data.Seconds
	}
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		seconds = 60
	}
	return time.Duration(min(86400, max(1, seconds)) * float64(time.Second))
}
