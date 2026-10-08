package rdbms

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

func (x sqliteIndexing) LockCoursesForMaintenance(ctx context.Context) error {
	// Id-ordered scan on the admitted writer serializes maintenance with
	// every other publish; SQLite takes no row locks.
	rows, err := x.db.Query(ctx, `SELECT id FROM courses ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
	}
	return rows.Err()
}

// indexingMaintenanceSelectSQLite renders the candidate SELECT with ? bindings.
func indexingMaintenanceSelectSQLite(action string) (string, []any) {
	query := `SELECT d.id FROM documents d JOIN courses c ON c.id=d.course_id` +
		` WHERE d.is_current=1 AND d.status NOT IN('unsupported','external','skipped_limit')` +
		` AND EXISTS(SELECT 1 FROM document_revisions r JOIN objects o ON o.id=r.object_id` +
		` WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path` +
		` AND r.deleted_at IS NULL AND o.sha256=d.source_hash)` +
		` AND (?='rebuild' OR d.status IN('pending','running','failed') OR d.content_hash_verified<>1` +
		` OR (coalesce(d.word_count,0)>0 AND NOT EXISTS(SELECT 1 FROM chunks ch WHERE ch.document_id=d.id))` +
		` OR EXISTS(SELECT 1 FROM control_commands q` +
		` WHERE q.queue='index' AND q.action='index_document' AND q.status='failed'` +
		` AND json_extract(q.payload,'$.document_id')=d.id)` +
		` OR EXISTS(SELECT 1 FROM chunks ch WHERE ch.document_id=d.id AND` +
		` (NOT EXISTS(SELECT 1 FROM chunks_fts f WHERE f.chunk_id=ch.id) OR` +
		` NOT EXISTS(SELECT 1 FROM chunk_embeddings e WHERE e.chunk_id=ch.id))))` +
		` AND (?<>'retry_failed' OR d.status='failed' OR EXISTS(SELECT 1 FROM control_commands q` +
		` WHERE q.queue='index' AND q.action='index_document' AND q.status='failed'` +
		` AND json_extract(q.payload,'$.document_id')=d.id))`
	return query, []any{action, action}
}

func (x sqliteIndexing) MaintainIndex(ctx context.Context, action string) error {
	switch action {
	case "reconcile", "rebuild", "retry_failed":
	default:
		return errors.New("unknown knowledge maintenance action")
	}
	selectQuery, args := indexingMaintenanceSelectSQLite(action)
	// Materialize the candidate ids first: the retry/admit/publish writes
	// below all predicate on the same maintenance snapshot, and the
	// writer transaction holds it through commit.
	rows, err := x.db.Query(ctx, selectQuery, args...)
	if err != nil {
		return err
	}
	candidates := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if err := x.retryIndexCommands(ctx, candidates); err != nil {
		return err
	}
	if err := x.admitIndexCommands(ctx, candidates); err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	marks := make([]string, 0, len(candidates))
	updateArgs := make([]any, 0, len(candidates))
	for _, id := range candidates {
		marks = append(marks, "?")
		updateArgs = append(updateArgs, id)
	}
	_, err = x.db.Exec(ctx, `UPDATE documents SET status='pending',error=NULL,diagnostic_reason=NULL`+
		` WHERE id IN (`+strings.Join(marks, ",")+`)`, updateArgs...)
	return err
}

func (x sqliteIndexing) retryIndexCommands(ctx context.Context, candidates []string) error {
	if len(candidates) == 0 {
		return nil
	}
	marks := make([]string, 0, len(candidates))
	args := make([]any, 0, len(candidates))
	for _, id := range candidates {
		marks = append(marks, "?")
		args = append(args, id)
	}
	_, err := x.db.Exec(ctx, `UPDATE control_commands`+
		` SET status='pending',attempts=0,error=NULL,claimed_at=NULL,`+
		`available_at=strftime('%Y-%m-%d %H:%M:%S','now')`+
		` WHERE queue='index' AND action='index_document' AND status='failed'`+
		` AND json_extract(payload,'$.document_id') IN (`+strings.Join(marks, ",")+`)`, args...)
	return err
}

func (x sqliteIndexing) admitIndexCommands(ctx context.Context, candidates []string) error {
	for _, id := range candidates {
		var pending int64
		err := x.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM control_commands q`+
			` WHERE q.queue='index' AND q.action='index_document'`+
			` AND json_extract(q.payload,'$.document_id')=? AND q.status IN('pending','running','failed'))`,
			id).Scan(&pending)
		if err != nil {
			return err
		}
		if pending == 1 {
			continue
		}
		commandID, err := indexingCommandID()
		if err != nil {
			return err
		}
		payload := `{"document_id":` + indexingJSONString(id) + `}`
		if _, err := x.db.Exec(ctx, `INSERT INTO control_commands(id,queue,action,payload)`+
			` VALUES(?, 'index', 'index_document', ?)`, commandID, payload); err != nil {
			return err
		}
	}
	return nil
}

// indexingCommandID renders one v4 UUID in the gen_random_uuid() text shape.
func indexingCommandID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	text := hex.EncodeToString(raw[:])
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:], nil
}

// indexingJSONString quotes one document id for the command payload. Only
// JSON string escapes occur; ids never carry control characters.
func indexingJSONString(id string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range id {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}

func (x sqliteIndexing) RetryFailedAnalyses(ctx context.Context) error {
	stamp := time.Now().UTC().Format("2006-01-02T15:04:05.00")
	for _, table := range []string{
		"document_enrichments",
		"page_enrichments",
		"course_blueprints",
		"practice_question_sets",
	} {
		if _, err := x.db.Exec(ctx, `UPDATE `+table+` SET status='pending',attempts=0,error=NULL,`+
			`claimed_at=NULL,available_at=? WHERE status='failed'`, stamp); err != nil {
			return err
		}
	}
	return nil
}
