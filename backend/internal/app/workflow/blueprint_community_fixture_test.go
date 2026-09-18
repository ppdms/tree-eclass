package workflow

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/messages"
)

func blueprintCommunityFixture(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Exec(ctx, `INSERT INTO app.discord_course_channels(root_channel_id,course_id) VALUES('100001',101);
 INSERT INTO messages.archive_sources(path,root_id,course_id,fingerprint,channel_id,status,indexed_at) VALUES('blueprint-fixture','100001',101,'fixture-sha',100002,'ready','2026-09-12');
 INSERT INTO messages.conversations(conversation_id,course_id,root_id,channel_id,channel_name,channel_type,first_message_id,last_message_id,started_at,ended_at,ended_at_epoch,text,normalized_text,participant_count,reaction_count,source_path)
 VALUES('two',101,100001,100002,'Synthetic discussion','text',100003,100003,'2026-09-12','2026-09-12',1789171201,'Contradictory fixture claim','contradictory fixture claim',1,0,'blueprint-fixture');
 INSERT INTO messages.messages(message_id,channel_id,course_id,timestamp,timestamp_epoch,author_name,content,searchable_text,message_type,source_path)
 VALUES(100003,100002,101,'2026-09-12',1789171201,'Fixture author','Contradictory fixture claim','contradictory fixture claim','Default','blueprint-fixture');
 INSERT INTO messages.conversation_messages(conversation_id,message_id,source_path,position) VALUES('two',100003,'blueprint-fixture',0)`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	hash, err := messages.Snapshot(ctx, tx, 101, "two")
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
