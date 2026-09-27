// Package inference implements bounded provider transports shared by Ask and
// source analysis. It owns no database, scheduling or tool execution.
package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	dinference "tree-eclass/internal/domain/inference"
)

// The wire types below are the shared provider contract from
// domain/inference; they remain identical types for every caller.
type (
	Candidate = dinference.Candidate
	Function  = dinference.Function
	ToolCall  = dinference.ToolCall
	Message   = dinference.Message
	Tool      = dinference.Tool
	Request   = dinference.Request
	Delta     = dinference.Delta
	Error     = dinference.Error
)

type Client struct{ HTTP *http.Client }

func New() *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost = 6, 1, 2
	transport.ResponseHeaderTimeout, transport.IdleConnTimeout = 45*time.Second, 30*time.Second
	return &Client{
		HTTP: &http.Client{
			Transport:     transport,
			Timeout:       180 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (c *Client) Close() { c.HTTP.CloseIdleConnections() }

func (c *Client) Stream(ctx context.Context, candidate Candidate, in Request, emit func(Delta) error) error {
	endpoint, err := url.Parse(candidate.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil ||
		(endpoint.Scheme != "https" && endpoint.Scheme != "http") {
		return errors.New("invalid inference endpoint")
	}
	if candidate.APIKey == "" || candidate.Model == "" || len(in.Messages) == 0 {
		return errors.New("inference requires a model, credential and messages")
	}
	payload, err := wirePayload(candidate, in)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(raw) > 16*1024*1024 {
		return errors.New("inference request exceeds 16 MiB")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+candidate.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream, application/x-ndjson")
	req.Header.Set("User-Agent", "tree-eclass/1.0 (Go native inference)")
	response, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &Error{Provider: candidate.Provider, Retryable: true, Reason: "provider connection failed"}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Provider error bodies may echo credentials or private source content.
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return &Error{
			Provider: candidate.Provider,
			Status:   response.StatusCode,
			Retryable: response.StatusCode == http.StatusTooManyRequests ||
				response.StatusCode == http.StatusRequestTimeout ||
				response.StatusCode >= 500,
			RetryAfter: RetryAfter(response.Header.Get("Retry-After")),
			Reason:     "provider rejected the request",
		}
	}
	if candidate.Provider == "ollama" {
		return streamOllama(response.Body, emit)
	}
	return streamSSE(response.Body, emit)
}

// RetryAfter is the shared domain retry-delay parser (quota + transports).
var RetryAfter = dinference.RetryAfter
