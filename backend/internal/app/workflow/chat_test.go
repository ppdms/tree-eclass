package workflow

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"tree-eclass/internal/services/chat"
)

func chatChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := context.Background()
	store := chat.Store{Pool: pool}
	first, err := store.SaveTurn(
		ctx,
		chat.Turn{
			Question:  "Πώς λειτουργούν τα δένδρα;",
			Answer:    "**Συνθετική** απάντηση. <script>bad()</script>",
			Model:     "synthetic-test",
			Consulted: []chat.Consultation{{Tool: "search_materials", Arguments: map[string]any{"query": "δένδρα"}}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var conversation chat.Conversation
	target := fmt.Sprintf("%s/api/ask/conversations/%d", base, first.ID)
	apiJSON(t, "GET", target, nil, 200, &conversation)
	if len(conversation.Messages) != 2 || !strings.Contains(conversation.Messages[1].ContentHTML, "<strong>") ||
		strings.Contains(conversation.Messages[1].ContentHTML, "<script>") {
		t.Fatal("conversation turn or safe markdown changed")
	}
	interrupted, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = store.SaveTurn(interrupted, chat.Turn{Question: "Must not appear", Answer: "No answer"}); err == nil {
		t.Fatal("cancelled turn committed")
	}
	var list struct {
		Conversations []chat.Summary `json:"conversations"`
	}
	apiJSON(t, "GET", base+"/api/ask/conversations", nil, 200, &list)
	if len(list.Conversations) != 1 || list.Conversations[0].MessageCount != 2 {
		t.Fatal("abandoned chat left a half-turn")
	}
	apiJSON(t, "PATCH", target, map[string]any{"title": " Νέος  τίτλος "}, 200, nil)
	apiJSON(t, "GET", target, nil, 200, &conversation)
	if conversation.Title != "Νέος τίτλος" {
		t.Fatal("conversation rename not normalized")
	}
	apiJSON(t, "DELETE", target, nil, 200, nil)
	apiJSON(t, "GET", target, nil, 404, nil)
	var messages int
	if err = pool.Native.QueryRow(ctx, `SELECT count(*) FROM app.chat_messages WHERE conversation_id=$1`, first.ID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 0 {
		t.Fatal("conversation deletion left orphaned messages")
	}
}
