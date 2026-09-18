package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/messages"
)

func TestNativeMessageEvidenceScope(t *testing.T) {
	t.Parallel()
	c := nativeSharedController(t)
	ctx := t.Context()
	conn, _ := startTestStorage(t, c)
	defer conn.Close(ctx)
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(
		ctx,
		`INSERT INTO app.courses(id,name,webdav_folder) VALUES(901,'Community course','/Courses/901'),(902,'Other course','/Courses/902');
 INSERT INTO app.discord_course_channels(root_channel_id,course_id) VALUES('90001',901),('90002',902);
 INSERT INTO messages.archive_sources(path,root_id,course_id,fingerprint,channel_id,status,indexed_at) VALUES
 ('current','90001',901,'f',11,'ready','2026-09-12'),('old','90001',901,'f-old',11,'ready','2026-09-01'),('foreign','90002',902,'other',12,'ready','2026-09-12');
 INSERT INTO messages.channels(channel_id,root_id,course_id,guild_id,channel_name,channel_type,source_path) VALUES
 (11,90001,901,99,'ανακοινώσεις','text','current'),(12,90002,902,99,'foreign','text','foreign');
 INSERT INTO messages.messages(message_id,channel_id,course_id,timestamp,timestamp_epoch,author_name,content,searchable_text,reply_to_message_id,message_type,source_path) VALUES
 (100,11,901,'2026-09-12',1789171200,'Author','New context','New context',NULL,'Default','current'),
 (100,11,901,'2026-09-01',1788220800,'Author','Old context','Old context',NULL,'Default','old'),
 (101,11,901,'2026-09-12',1789171201,'Author','Course evidence','Course evidence',777,'Default','current'),
 (102,11,901,'2026-09-12',1789171202,'Author','After','After',NULL,'Default','current'),
 (777,12,902,'2026-09-12',1789171200,'Foreign author','Private other course','Private',NULL,'Default','foreign');
 INSERT INTO messages.conversations(conversation_id,course_id,root_id,channel_id,channel_name,channel_type,first_message_id,last_message_id,started_at,ended_at,ended_at_epoch,text,normalized_text,participant_count,reaction_count,source_path)
 VALUES('conversation',901,90001,11,'ανακοινώσεις','text',101,101,'2026-09-12','2026-09-12',1789171201,'Course evidence','course evidence',1,0,'current');
 INSERT INTO messages.conversation_messages(conversation_id,message_id,source_path,position) VALUES('conversation',101,'current',0)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	text := "Δένδρα\x00\ue000"
	if _, err = pool.Exec(ctx, `UPDATE messages.messages SET content=$1 WHERE message_id=101`, identity.Encode(text)); err != nil {
		t.Fatal(err)
	}
	service := messages.Reader{Pool: pool}
	read, err := service.Read(ctx, "conversation", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Messages) != 1 || read.Messages[0].Content != text || len(read.Before) != 1 ||
		read.Before[0].Content != "New context" ||
		len(read.After) != 1 ||
		len(read.Replies) != 0 ||
		read.Truncated {
		t.Fatal("message scope, chronology or identity", read)
	}
	if read.Messages[0].URL == nil || *read.Messages[0].URL != "https://discord.com/channels/99/11/101" ||
		read.Conversation.Evidence != "community_discussion" ||
		!read.Messages[0].Untrusted {
		t.Fatal("community provenance", read)
	}
	status, err := service.Status(ctx, []int64{901})
	if err != nil || !status.Available || len(status.Courses) != 1 || status.Totals["messages"] != 4 ||
		status.Totals["conversations"] != 1 {
		t.Fatal(status, err)
	}
	messageSearchChecks(t, pool, service)
	communityFreshnessChecks(t, pool)
	if _, err = pool.Exec(ctx, `DELETE FROM app.discord_course_channels WHERE root_channel_id='90001'`); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Read(ctx, "conversation", 1, 1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("removed mapping still readable", err)
	}
	status, err = service.Status(ctx, []int64{901})
	if err != nil || status.Available || len(status.Courses) != 0 || status.Totals["messages"] != 0 {
		t.Fatal("removed mapping still reported as current archive", status, err)
	}
	search, err := service.Search(ctx, messages.SearchRequest{Query: "Δένδρα", Courses: []int64{901}}, time.Now())
	if err != nil || search.Available || len(search.Results) != 0 {
		t.Fatal("removed mapping still searchable", search, err)
	}
}

func messageSearchChecks(t *testing.T, pool *pgxpool.Pool, service messages.Reader) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `UPDATE messages.conversations SET text='Τα δένδρα και οι βαθμοί',normalized_text='τα δενδρα και οι βαθμοι' WHERE conversation_id='conversation';
 INSERT INTO messages.conversations_fts(conversation_id,text,normalized_text,channel_name) VALUES('conversation','Τα δένδρα και οι βαθμοί','τα δενδρα και οι βαθμοι','ανακοινώσεις')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO messages.conversation_embeddings(conversation_id,model,vector,dimensions) VALUES('conversation',$1,$2,384)`, knowledge.LocalEmbeddingModel, knowledge.Pack(knowledge.Embed("Τα δένδρα και οι βαθμοί"))); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"lexical", "semantic", "hybrid"} {
		found, err := service.Search(
			ctx,
			messages.SearchRequest{Query: "Πώς είναι τα δένδρα", Courses: []int64{901}, Mode: mode},
			time.Now(),
		)
		if err != nil || len(found.Results) != 1 {
			t.Fatal(mode, found, err)
		}
		hit := found.Results[0]
		if hit.ID != "conversation" || hit.URL == nil || *hit.URL != "https://discord.com/channels/99/11/101" ||
			hit.Count != 1 ||
			hit.IDs[0] != "101" ||
			hit.Score <= 0 ||
			hit.Year == nil ||
			*hit.Year != "2026-27" ||
			!hit.Untrusted ||
			hit.SourceWeight != .95 {
			t.Fatal("community ranking or provenance", hit)
		}
	}
	if _, err := service.Search(ctx, messages.SearchRequest{Query: "Δένδρα", Mode: "invalid"}, time.Now()); err == nil {
		t.Fatal("invalid search mode accepted")
	}
	if _, err := service.Search(ctx, messages.SearchRequest{Query: "Δένδρα", Courses: []int64{999999}}, time.Now()); !errors.Is(
		err,
		knowledge.ErrUnavailable,
	) {
		t.Fatal("unknown course search accepted", err)
	}
}
