package library

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestReadOnlyStdioBridge(t *testing.T) {
	r, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	tool := r.tools["list_courses"]
	tool.Handler = func(context.Context, json.RawMessage) (any, error) {
		return map[string]any{"courses": []string{"Συνθετικό"}}, nil
	}
	r.tools["list_courses"] = tool
	httpServer := httptest.NewServer(r.HTTP())
	defer httpServer.Close()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- Proxy(t.Context(), httpServer.URL+"/mcp", serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_courses"})
	if err != nil || result.IsError || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "Συνθετικό") {
		t.Fatal(result, err)
	}
	session.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestStdioFrameBound(t *testing.T) {
	reader := &frameReader{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", 128*1024+1)))}
	if _, err := io.Copy(io.Discard, reader); err == nil {
		t.Fatal("unterminated oversized frame accepted")
	}
	reader = &frameReader{
		ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat(strings.Repeat("x", 1024)+"\n", 200))),
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatal("independent small frames accumulated", err)
	}
}
