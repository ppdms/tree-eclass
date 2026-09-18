package knowledge

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/storage/queries"
)

// Maintain runs on the same serial queue as extraction. Rebuilding replaces
// derived chunks during successful indexing; document IDs, revisions, source
// bytes, annotations, AI evidence and learner history remain intact.
func (s Reader) Maintain(ctx context.Context, action string) error {
	if action != "reconcile" && action != "rebuild" && action != "retry_failed" {
		return errors.New("unknown knowledge maintenance action")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Match domain write ordering before taking document/derived-generation locks.
	if _, err = tx.Exec(ctx, `SELECT id FROM app.courses ORDER BY id FOR UPDATE`); err != nil {
		return err
	}
	if err = queries.New(tx).QueueLock(ctx, "index"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, maintenanceQuery, action); err != nil {
		return err
	}
	if action == "retry_failed" {
		if err = retryAnalyses(ctx, tx); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const maintenanceQuery = `WITH candidates AS MATERIALIZED (
 SELECT d.id FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id
 WHERE d.is_current=1 AND d.status NOT IN('unsupported','external','skipped_limit')
 AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
 WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path AND r.deleted_at IS NULL AND o.sha256=d.source_hash)
 AND ($1='rebuild' OR d.status IN('pending','running','failed') OR d.content_hash_verified<>1
 OR (coalesce(d.word_count,0)>0 AND NOT EXISTS(SELECT 1 FROM knowledge.chunks ch WHERE ch.document_id=d.id))
 OR EXISTS(SELECT 1 FROM app.control_commands q WHERE q.queue='index' AND q.action='index_document' AND q.status='failed' AND q.payload->>'document_id'=d.id)
 OR EXISTS(SELECT 1 FROM knowledge.chunks ch WHERE ch.document_id=d.id AND
 (NOT EXISTS(SELECT 1 FROM knowledge.chunks_fts f WHERE f.chunk_id=ch.id) OR
 NOT EXISTS(SELECT 1 FROM knowledge.chunk_embeddings e WHERE e.chunk_id=ch.id))))
 AND ($1<>'retry_failed' OR d.status='failed' OR EXISTS(SELECT 1 FROM app.control_commands q WHERE q.queue='index' AND q.action='index_document' AND q.status='failed' AND q.payload->>'document_id'=d.id))
), retried AS (
 UPDATE app.control_commands q SET status='pending',attempts=0,error=NULL,claimed_at=NULL,available_at=now()
 WHERE q.queue='index' AND q.action='index_document' AND q.status='failed' AND q.payload->>'document_id' IN(SELECT id FROM candidates)
), admitted AS (
 INSERT INTO app.control_commands(id,queue,action,payload)
 SELECT gen_random_uuid()::text,'index','index_document',jsonb_build_object('document_id',d.id)
 FROM candidates d WHERE NOT EXISTS(SELECT 1 FROM app.control_commands q WHERE q.queue='index' AND q.action='index_document' AND q.payload->>'document_id'=d.id AND q.status IN('pending','running','failed'))
)
 UPDATE knowledge.documents SET status='pending',error=NULL,diagnostic_reason=NULL WHERE id IN(SELECT id FROM candidates)`

func retryAnalyses(ctx context.Context, tx pgx.Tx) error {
	// This finite internal allowlist is the only source of SQL identifiers.
	for _, table := range []string{"document_enrichments", "page_enrichments", "course_blueprints", "practice_question_sets"} {
		if _, err := tx.Exec(ctx, `UPDATE knowledge.`+table+` SET status='pending',attempts=0,error=NULL,claimed_at=NULL,available_at=to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US') WHERE status='failed'`); err != nil {
			return err
		}
	}
	return nil
}
