package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"time"
	"tree-eclass/internal/app/server"
	"tree-eclass/internal/infrastructure/quota"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/services/chat"
)

type syntheticInference func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error

func (f syntheticInference) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	emit func(inference.Delta) error,
) error {
	return f(ctx, c, r, emit)
}

func TestNativeAskCompleteAndAbandonedTurns(t *testing.T) {
	t.Parallel()
	c := nativeSharedController(t)
	ctx := t.Context()
	conn, _ := startTestStorage(t, c)
	defer conn.Close(ctx)
	pool := newFixtureStore(t, ctx, c)
	defer pool.Close()
	entered, abandoned := make(chan struct{}), make(chan struct{})
	endpoint := newAskFixtureServer(t, c, ctx, entered, abandoned)
	store := chat.Store{Pool: pool}
	conversationID := askCompleteChecks(t, ctx, endpoint, store)
	askFailureChecks(t, ctx, endpoint)
	askCancellationChecks(t, ctx, endpoint, entered, abandoned, store)
	quotaPersistenceChecks(t, pool)
	historyChecks(t, pool, store, conversationID)
}

func newAskFixtureServer(
	t *testing.T,
	c *Controller,
	ctx context.Context,
	entered, abandoned chan struct{},
) string {
	t.Helper()
	api, err := server.New(
		ctx,
		server.Config{
			DatabaseURL: c.databaseURL(),
			ObjectsRoot: c.testObjectsRoot(),
			Mode:        "test", Session: "test-runtime",
			ProviderKeys: map[string]string{"SYNTHETIC_API_KEY": "synthetic-not-a-real-key"},
		},
		server.WithInferenceClient(askFixtureInference(entered, abandoned)),
	)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api)
	t.Cleanup(func() {
		httpServer.Close()
		api.Close()
	})
	return httpServer.URL + "/api/ask/stream"
}

func askFixtureInference(entered, abandoned chan struct{}) syntheticInference {
	return func(ctx context.Context, _ inference.Candidate, in inference.Request, emit func(inference.Delta) error) error {
		question := ""
		for _, m := range in.Messages {
			if m.Role == "user" {
				question = m.Content.(string)
			}
		}
		if question == "fail" {
			_ = emit(inference.Delta{Text: "partial"})
			return errors.New("fixture secret must not be echoed")
		}
		if question == "abandon" {
			close(entered)
			<-ctx.Done()
			close(abandoned)
			return ctx.Err()
		}
		if in.Messages[len(in.Messages)-1].Role != "tool" {
			return emit(inference.Delta{
				Finish: "tool_calls",
				Calls: []inference.ToolCall{{ID: "same-id", Type: "function", Function: inference.Function{
					Name: "list_courses", Arguments: `{}`,
				}}},
			})
		}
		if !strings.Contains(in.Messages[len(in.Messages)-1].Content.(string), "courses") {
			return errors.New("missing real registry result")
		}
		if err := emit(inference.Delta{Text: "Συνθετική απάντηση"}); err != nil {
			return err
		}
		return emit(inference.Delta{Finish: "stop"})
	}
}

func askCompleteChecks(t *testing.T, ctx context.Context, endpoint string, store chat.Store) int64 {
	t.Helper()
	response := askRequest(t, ctx, endpoint, `{"question":"hello"}`, "test-runtime")
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("X-Vercel-AI-UI-Message-Stream") != "v1" ||
		!strings.Contains(string(raw), "tool-input-available") ||
		!strings.Contains(string(raw), "data-conversation") ||
		!strings.HasSuffix(string(raw), "data: [DONE]\n\n") {
		t.Fatal("UI stream", response.StatusCode, string(raw))
	}
	conversations, err := store.List(ctx, 50)
	if err != nil || len(conversations) != 1 || conversations[0].MessageCount != 2 {
		t.Fatal(conversations, err)
	}
	conversation, err := store.Get(ctx, conversations[0].ID)
	if err != nil || conversation.Messages[1].Content != "Συνθετική απάντηση" ||
		len(conversation.Messages[1].Consulted) != 1 {
		t.Fatal(conversation, err)
	}
	return conversations[0].ID
}

func askFailureChecks(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	for _, test := range []struct {
		body, token string
		status      int
	}{
		{`{"question":"fail"}`, "test-runtime", 200}, {`{"question":"hello"}`, "stale", 409},
		{`{"question":""}`, "test-runtime", 400}, {`{"question":"hi","conversation_id":"missing"}`, "test-runtime", 400},
		{`{"question":"hi","conversation_id":99999}`, "test-runtime", 404},
	} {
		response := askRequest(t, ctx, endpoint, test.body, test.token)
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != test.status || strings.Contains(string(raw), "fixture secret") {
			t.Fatal(test, response.StatusCode, string(raw))
		}
		if test.status == 200 &&
			(!strings.Contains(string(raw), `"type":"error"`) || strings.Contains(string(raw), "data-conversation")) {
			t.Fatal("failed turn reported success", string(raw))
		}
	}
}

func askCancellationChecks(
	t *testing.T,
	ctx context.Context,
	endpoint string,
	entered, abandoned chan struct{},
	store chat.Store,
) {
	t.Helper()
	cancelCtx, cancel := context.WithCancel(ctx)
	response := askRequest(t, cancelCtx, endpoint, `{"question":"abandon"}`, "test-runtime")
	<-entered
	busy := askRequest(t, ctx, endpoint, `{"question":"hello"}`, "test-runtime")
	busy.Body.Close()
	if busy.StatusCode != 409 {
		t.Fatal("parallel answers admitted")
	}
	cancel()
	response.Body.Close()
	<-abandoned
	conversations, err := store.List(ctx, 50)
	if err != nil || len(conversations) != 1 || conversations[0].MessageCount != 2 {
		t.Fatal("failed or abandoned turn persisted", conversations, err)
	}
}

func askRequest(t *testing.T, ctx context.Context, endpoint, body, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tree-Runtime", token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
func historyChecks(t *testing.T, pool *fixtureStore, store chat.Store, id int64) {
	t.Helper()
	ctx := t.Context()
	for range 12 {
		if _, err := store.SaveTurn(ctx, chat.Turn{ConversationID: &id, Question: "q", Answer: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	history, err := store.History(ctx, id)
	if err != nil || len(history) != 20 {
		t.Fatal("history not bounded", len(history), err)
	}
	if _, err = pool.Native.Exec(ctx, `UPDATE app.chat_messages SET content=repeat('x',400001) WHERE id=(SELECT max(id) FROM app.chat_messages WHERE conversation_id=$1)`, id); err != nil {
		t.Fatal(err)
	}
	history, err = store.History(ctx, id)
	if err != nil || len(history) != 0 {
		raw, _ := json.Marshal(history)
		t.Fatal("oversized context admitted", len(raw), err)
	}
}

func quotaPersistenceChecks(t *testing.T, pool *fixtureStore) {
	t.Helper()
	calls := 0
	fake := syntheticInference(
		func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
			calls++
			return &inference.Error{Status: 429, Retryable: true, RetryAfter: time.Hour}
		},
	)
	g := &quota.Guard{Store: quota.Store{Pool: pool}, Client: fake}
	candidate := inference.Candidate{Provider: "huggingface", APIKey: "must-not-enter-database"}
	_ = g.Stream(t.Context(), candidate, inference.Request{}, nil)
	g = &quota.Guard{Store: quota.Store{Pool: pool}, Client: fake}
	if err := g.Stream(t.Context(), candidate, inference.Request{}, nil); err == nil || calls != 1 {
		t.Fatal("database pause lost on restart", calls, err)
	}
	var raw string
	if err := pool.Native.QueryRow(t.Context(), `SELECT value FROM knowledge.knowledge_state WHERE key='huggingface_quota'`).Scan(&raw); err != nil ||
		strings.Contains(raw, candidate.APIKey) {
		t.Fatal("unsafe quota persistence", err)
	}
}
