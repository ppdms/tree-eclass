package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"tree-eclass/internal/services/library"
)

func libraryChecks(t *testing.T, pool *fixtureStore, base, document string) {
	t.Helper()
	ctx := t.Context()
	r, err := library.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"list_courses": `{}`, "list_materials": `{"course_id":101,"include_insights":true}`,
		"search_materials":           `{"query":"Δένδρα","course_ids":[101]}`,
		"search_course_messages":     `{"query":"Δένδρα","course_ids":[101]}`,
		"read_course_messages":       `{"conversation_id":"absent"}`,
		"search_course_knowledge":    `{"query":"Δένδρα","course_ids":[101]}`,
		"read_material":              `{"document_id":"` + document + `"}`,
		"get_material_insight":       `{"document_id":"` + document + `"}`,
		"get_page_insight":           `{"document_id":"` + document + `","page_number":1}`,
		"get_study_priorities":       `{"course_ids":[101]}`,
		"get_course_study_blueprint": `{"course_id":101}`,
		"get_recent_changes":         `{"course_ids":[101]}`,
		"get_index_status":           `{"course_ids":[101]}`,
		"get_message_index_status":   `{"course_ids":[101]}`,
	}
	for name, args := range cases {
		_, err = r.Call(ctx, name, json.RawMessage(args))
		wantErr := name == "read_course_messages" || name == "get_page_insight"
		if (err != nil) != wantErr {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for _, uri := range []string{"eclass://courses", "eclass://courses/101", "eclass://courses/101/guide", "eclass://documents/" + document, "eclass://documents/" + document + "/insight", "eclass://documents/" + document + "/units/line:1"} {
		if _, err = r.Resource(ctx, uri); err != nil {
			t.Fatal(uri, err)
		}
	}
	libraryMCPChecks(t, ctx, base, document)
}

func libraryMCPChecks(t *testing.T, ctx context.Context, base, document string) {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "synthetic-test", Version: "1"}, nil)
	session, err := client.Connect(
		ctx,
		&mcp.StreamableClientTransport{Endpoint: base + "/mcp", DisableStandaloneSSE: true},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := session.ListTools(ctx, nil)
	if err != nil || len(listed.Tools) != 14 {
		t.Fatal("MCP discovery", listed, err)
	}
	for _, tool := range listed.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || *tool.Annotations.OpenWorldHint ||
			*tool.Annotations.DestructiveHint {
			t.Fatal("unsafe annotation", tool)
		}
	}
	called, err := session.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "read_material", Arguments: map[string]any{"document_id": document}},
	)
	if err != nil || called.IsError {
		t.Fatal(called, err)
	}
	called, err = session.CallTool(
		ctx,
		&mcp.CallToolParams{
			Name:      "read_material",
			Arguments: map[string]any{"document_id": document, "locators": []string{"page:1"}},
		},
	)
	if err != nil || !called.IsError {
		t.Fatal("invalid arguments accepted", called, err)
	}
	resource, err := session.ReadResource(ctx, &mcp.ReadResourceParams{URI: "eclass://documents/" + document})
	if err != nil || len(resource.Contents) != 1 {
		t.Fatal(resource, err)
	}
}
