package messages

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Snapshot binds saved synthesis to the exact mapped conversation and message
// records. The hash is computed inside the caller's snapshot without exporting
// archive paths, unbounded messages or provider data to the application heap.
func Snapshot(ctx context.Context, tx pgx.Tx, course int64, id string) (string, error) {
	var hash string
	err := tx.QueryRow(ctx, `SELECT encode(sha256(convert_to(jsonb_build_array(to_jsonb(c),a.fingerprint,
 (SELECT string_agg(encode(sha256(convert_to(jsonb_build_array(cm.message_id,cm.source_path,cm.position,to_jsonb(m))::text,'UTF8')),'hex'),',' ORDER BY cm.position,cm.message_id,cm.source_path)
 FROM messages.conversation_messages cm LEFT JOIN messages.messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path WHERE cm.conversation_id=c.conversation_id))::text,'UTF8')),'hex')
 FROM messages.conversations c JOIN app.courses co ON co.id=c.course_id
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=c.root_id::text AND a.status='ready'
 WHERE c.course_id=$1 AND c.conversation_id=$2 AND (co.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=co.id AND p.enabled=1))`, course, id).Scan(&hash)
	return hash, err
}
