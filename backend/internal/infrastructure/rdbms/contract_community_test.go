package rdbms_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
)

func TestCommunityStatusAndSemanticCandidates(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			seedCommunityArchive(t, store)
			assertCommunityStatus(t, store)
			assertCommunitySemantic(t, store)
		})
	}
}

func assertCommunityStatus(t *testing.T, store database.Store) {
	t.Helper()
	rows, err := store.Community().CourseStatus(t.Context(), []int64{812, 811})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CourseID != 811 {
		t.Fatalf("community status scope = %+v", rows)
	}
	row := rows[0]
	if row.Messages != 2 || row.Conversations != 2 || row.Sources != 1 || row.FailedSources != 0 {
		t.Fatalf("community status counts = %+v", row)
	}
	if row.Latest == nil || *row.Latest != "2026-09-12T11:01:00Z" {
		t.Fatalf("community latest = %+v", row.Latest)
	}
	if rows, err = store.Community().CourseStatus(t.Context(), nil); err != nil || len(rows) != 0 {
		t.Fatalf("empty community status = %v, %v", rows, err)
	}
}

func assertCommunitySemantic(t *testing.T, store database.Store) {
	t.Helper()
	want := []string{"conv_old", "conv_recent"}
	zero := collectCommunityIDs(t, store, []string{})
	if !reflect.DeepEqual(zero, want) {
		t.Fatalf("semantic zero lexical = %v", zero)
	}
	multi := collectCommunityIDs(t, store, []string{"conv_recent", "conv_old"})
	if !reflect.DeepEqual(multi, want) {
		t.Fatalf("semantic multiple lexical = %v", multi)
	}
	dup := collectCommunityIDs(t, store, []string{"conv_old", "conv_old"})
	if !reflect.DeepEqual(dup, want) {
		t.Fatalf("semantic duplicate lexical = %v", dup)
	}
}

func collectCommunityIDs(t *testing.T, store database.Store, lexical []string) []string {
	t.Helper()
	stream, err := store.Community().SemanticCandidates(t.Context(), []int64{811}, lexical,
		knowledge.LocalEmbeddingModel, knowledge.EmbeddingDimensions)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	ids := []string{}
	for stream.Next() {
		item := stream.Value()
		text := "old exam notes"
		if item.ConversationID == "conv_recent" {
			text = "recent exam notes"
		}
		if item.Excerpt != text || item.CourseID != 811 {
			t.Fatalf("semantic candidate = %+v", item.CommunityCandidate)
		}
		if !reflect.DeepEqual(item.Vector, knowledge.Pack(knowledge.Embed(text))) {
			t.Fatalf("semantic vector content changed for %s", item.ConversationID)
		}
		ids = append(ids, item.ConversationID)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

func seedCommunityArchive(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	if err := (courses.Service{Pool: store}).Add(ctx, 811, "community"); err != nil {
		t.Fatal(err)
	}
	if err := (courses.Service{Pool: store}).Add(ctx, 812, "unmapped"); err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Settings().ReplaceDiscordMapping(ctx, map[string]int64{"501": 811}); err != nil {
		t.Fatal(err)
	}
	path := publishCommunityArchive(t, ctx, tx)
	publishCommunityConversations(t, ctx, tx, path)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func publishCommunityArchive(t *testing.T, ctx context.Context, tx database.Tx) string {
	t.Helper()
	path, hash := "discord/501/502/contract.json", identity.TextHash("community-archive")
	if err := tx.Objects().RegisterObject(ctx, database.ObjectReference{
		Bucket: "contract", Key: hash, VersionID: hash, SHA256: hash, Bytes: 8,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.DiscordImports().BeginStage(ctx); err != nil {
		t.Fatal(err)
	}
	first, second := "2026-09-12T11:00:00Z", "2026-09-12T11:01:00Z"
	staged := []database.DiscordStagedMessage{
		{ID: 601, Timestamp: first, Epoch: 1, AuthorName: identity.Encode("author"),
			Content: identity.Encode("exam material"), SearchText: identity.Encode("exam"),
			MessageType: "Default", Attachments: "[]"},
		{ID: 602, Timestamp: second, Epoch: 2, AuthorName: identity.Encode("author"),
			Content: identity.Encode("exam material"), SearchText: identity.Encode("exam"),
			MessageType: "Default", Attachments: "[]"},
	}
	if err := tx.DiscordImports().AppendStagedMessages(ctx, staged); err != nil {
		t.Fatal(err)
	}
	if err := tx.DiscordImports().PublishArchive(ctx, database.DiscordArchivePublication{
		Path: path, RootID: 501, ChannelID: 502, CourseID: 811, GuildID: 503,
		ChannelName: identity.Encode("general"), ChannelType: "GuildTextChat",
		ExportedAt: second, ObjectID: hash,
	}); err != nil {
		t.Fatal(err)
	}
	return path
}

func publishCommunityConversations(t *testing.T, ctx context.Context, tx database.Tx, path string) {
	t.Helper()
	first, second := "2026-09-12T11:00:00Z", "2026-09-12T11:01:00Z"
	oldText, recentText := "old exam notes", "recent exam notes"
	conversations := []database.DiscordConversation{
		{ID: "conv_old", CourseID: 811, RootID: 501, ChannelID: 502,
			ChannelName: identity.Encode("general"), ChannelType: "GuildTextChat",
			FirstMessageID: 601, LastMessageID: 601, StartedAt: first, EndedAt: first,
			EndedAtEpoch: 1, Text: identity.Encode(oldText), NormalizedText: identity.Encode(oldText),
			MetadataJSON: "{}", SourcePath: path, MessageIDs: []int64{601},
			EmbeddingModel:      knowledge.LocalEmbeddingModel,
			EmbeddingVector:     knowledge.Pack(knowledge.Embed(oldText)),
			EmbeddingDimensions: knowledge.EmbeddingDimensions},
		{ID: "conv_recent", CourseID: 811, RootID: 501, ChannelID: 502,
			ChannelName: identity.Encode("general"), ChannelType: "GuildTextChat",
			FirstMessageID: 602, LastMessageID: 602, StartedAt: second, EndedAt: second,
			EndedAtEpoch: 2, Text: identity.Encode(recentText),
			NormalizedText:   identity.Encode(recentText),
			ParticipantCount: 1, MetadataJSON: "{}", SourcePath: path, MessageIDs: []int64{602},
			EmbeddingModel:      knowledge.LocalEmbeddingModel,
			EmbeddingVector:     knowledge.Pack(knowledge.Embed(recentText)),
			EmbeddingDimensions: knowledge.EmbeddingDimensions},
	}
	for _, conversation := range conversations {
		if err := tx.DiscordImports().PublishConversation(ctx, conversation); err != nil {
			t.Fatal(err)
		}
	}
}
