package library

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Proxy keeps stdio clients on the active API. It opens no database or storage
// handles, so clients can stay connected while the controller stops or switches.
func Proxy(ctx context.Context, endpoint string, transport mcp.Transport) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Path != "/mcp" || u.User != nil ||
		u.RawQuery != "" {
		return errors.New("MCP bridge requires the configured loopback API")
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "tree-eclass-stdio", Version: "1.0.0"}, nil)
	httpClient := &http.Client{
		Timeout:       35 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	upstream, err := client.Connect(
		ctx,
		&mcp.StreamableClientTransport{
			Endpoint:             endpoint,
			HTTPClient:           httpClient,
			DisableStandaloneSSE: true,
			MaxRetries:           -1,
		},
		nil,
	)
	if err != nil {
		return errors.New("the native library API is unavailable; start Tree-eClass manually first")
	}
	defer upstream.Close()
	defer httpClient.CloseIdleConnections()
	server, err := proxyServer(ctx, upstream)
	if err != nil {
		return err
	}
	return server.Run(ctx, transport)
}
func proxyServer(ctx context.Context, upstream *mcp.ClientSession) (*mcp.Server, error) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "tree-eclass", Version: "1.0.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Tools:     &mcp.ToolCapabilities{},
				Resources: &mcp.ResourceCapabilities{},
			},
		},
	)
	tools, err := upstream.ListTools(ctx, nil)
	if err != nil {
		return nil, err
	}
	if len(tools.Tools) > 100 || tools.NextCursor != "" {
		return nil, errors.New("unexpected library tool inventory")
	}
	for _, tool := range tools.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			return nil, errors.New("library bridge refuses a tool without read-only semantics")
		}
		server.AddTool(tool, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.Params.Arguments
			if len(args) == 0 {
				args = json.RawMessage(`{}`)
			}
			return upstream.CallTool(ctx, &mcp.CallToolParams{Name: tool.Name, Arguments: args})
		})
	}
	resources, err := upstream.ListResources(ctx, nil)
	if err != nil {
		return nil, err
	}
	templates, err := upstream.ListResourceTemplates(ctx, nil)
	if err != nil {
		return nil, err
	}
	if len(resources.Resources) > 100 || resources.NextCursor != "" || len(templates.ResourceTemplates) > 100 ||
		templates.NextCursor != "" {
		return nil, errors.New("unexpected library resource inventory")
	}
	read := func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return upstream.ReadResource(ctx, &mcp.ReadResourceParams{URI: req.Params.URI})
	}
	for _, resource := range resources.Resources {
		server.AddResource(resource, read)
	}
	for _, template := range templates.ResourceTemplates {
		server.AddResourceTemplate(template, read)
	}
	return server, nil
}
