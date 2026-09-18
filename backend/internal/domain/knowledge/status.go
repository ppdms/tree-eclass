package knowledge

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func (s Reader) Status(ctx context.Context, course *int64) (map[string]any, error) {
	var requested []int64
	if course != nil {
		requested = []int64{*course}
	}
	return s.StatusFor(ctx, requested)
}

func (s Reader) StatusFor(ctx context.Context, requested []int64) (map[string]any, error) {
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	for key, query := range statusQueries {
		rows, err := statusRows(ctx, tx, query, ids)
		if err != nil {
			return nil, err
		}
		result[key] = rows
	}
	if err = guideStatus(ctx, tx, ids, a, result); err != nil {
		return nil, err
	}
	roadmaps := []map[string]any{}
	for _, id := range ids {
		ready, err := Readiness(ctx, tx, id, a)
		if err != nil {
			return nil, err
		}
		var status string
		err = tx.QueryRow(ctx, `SELECT coalesce((SELECT b.status FROM knowledge.course_blueprints b WHERE b.course_id=$1 AND b.requested_model=$2 AND b.analysis_version=$3 ORDER BY revision DESC LIMIT 1),'missing')`, id, a.CourseModel, settings.CourseAnalysisVersion).
			Scan(&status)
		if err != nil {
			return nil, err
		}
		roadmaps = append(roadmaps, map[string]any{"course_id": id, "status": status, "readiness": ready})
	}
	result["roadmap_diagnostics"] = roadmaps
	result["ai_pipeline"] = map[string]any{
		"enabled":          a.EnrichmentEnabled,
		"model":            a.Model,
		"course_model":     a.CourseModel,
		"course_enabled":   a.CourseEnabled,
		"practice_enabled": a.PracticeEnabled,
	}
	result["embedding"], err = embeddingStatus(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	result["diagnostics_truncated"] = len(result["failed_documents"].([]map[string]any)) > 500 ||
		len(result["unsupported_documents"].([]map[string]any)) > 500
	for _, key := range []string{"failed_documents", "unsupported_documents"} {
		rows := result[key].([]map[string]any)
		result[key] = rows[:min(500, len(rows))]
	}
	return result, tx.Commit(ctx)
}

// Queries select metadata only and cap diagnostic lists before JSON conversion.
var statusQueries = map[string]string{
	"coverage": `SELECT to_jsonb(v) FROM (SELECT c.id course_id,
 count(d.id) FILTER(WHERE d.status NOT IN('unsupported','external')) supported_documents,
 count(d.id) FILTER(WHERE d.status='ready') indexed_documents,
 count(d.id) FILTER(WHERE d.status IN('failed','skipped_limit')) failed_documents,
 count(d.id) FILTER(WHERE d.status IN('pending','running')) pending_documents
 FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1 WHERE c.id=ANY($1::bigint[]) GROUP BY c.id ORDER BY c.id) v`,
	"documents":             `SELECT to_jsonb(v) FROM (SELECT status,count(*) count FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 GROUP BY status ORDER BY status) v`,
	"jobs":                  `SELECT to_jsonb(v) FROM (SELECT q.status,count(*) count FROM app.control_commands q JOIN knowledge.documents d ON d.id=q.payload->>'document_id' WHERE q.queue='index' AND d.course_id=ANY($1::bigint[]) AND d.is_current=1 GROUP BY q.status ORDER BY q.status) v`,
	"failed_documents":      `SELECT to_jsonb(v) FROM (SELECT id document_id,course_id,display_name,source_path,status,diagnostic_reason,left(error,1000) error FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 AND status='failed' ORDER BY course_id,source_path LIMIT 501) v`,
	"unsupported_documents": `SELECT to_jsonb(v) FROM (SELECT id document_id,course_id,display_name,source_path,status,diagnostic_reason,left(error,1000) error FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 AND status IN('unsupported','skipped_limit') ORDER BY course_id,source_path LIMIT 501) v`,
}

func statusRows(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err = decoder.Decode(&item); err != nil {
			return nil, err
		}
		for _, key := range []string{"display_name", "source_path", "course_name", "error"} {
			if text, ok := item[key].(string); ok {
				item[key] = identity.Decode(text)
			}
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func embeddingStatus(ctx context.Context, tx pgx.Tx, ids []int64) (map[string]any, error) {
	var chunks, embedded int64
	err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE EXISTS(SELECT 1 FROM knowledge.chunk_embeddings e WHERE e.chunk_id=c.id AND e.model=$2)) FROM knowledge.chunks c JOIN knowledge.documents d ON d.id=c.document_id WHERE d.course_id=ANY($1::bigint[]) AND d.is_current=1 AND d.status='ready'`, ids, LocalEmbeddingModel).
		Scan(&chunks, &embedded)
	return map[string]any{
		"model":           LocalEmbeddingModel,
		"dimensions":      EmbeddingDimensions,
		"chunks":          chunks,
		"embedded_chunks": embedded,
		"missing_chunks":  chunks - embedded,
	}, err
}

func guideStatus(ctx context.Context, tx pgx.Tx, ids []int64, a settings.AI, result map[string]any) error {
	args := []any{ids, a.Model, settings.DocumentAnalysisVersion, settings.PageSynthesisVersion}
	counts, err := statusRows(
		ctx,
		tx,
		guideStatusCTE+`SELECT to_jsonb(v) FROM(SELECT status,count(*) count FROM guides GROUP BY status ORDER BY status) v`,
		args...)
	if err != nil {
		return err
	}
	rows, err := statusRows(
		ctx,
		tx,
		guideStatusCTE+`SELECT to_jsonb(v) FROM(SELECT * FROM guides WHERE status<>'ready' ORDER BY course_id,source_path LIMIT 501) v`,
		args...)
	if err != nil {
		return err
	}
	result["guide_summary"], result["guide_diagnostics"], result["guide_diagnostics_truncated"] = counts, rows[:min(500, len(rows))], len(
		rows,
	) > 500
	return nil
}

const guideStatusCTE = `WITH guides AS (
 SELECT d.id document_id,d.course_id,d.display_name,d.source_path,e.model,left(e.error,1000) error,
 CASE WHEN e.document_id IS NULL THEN 'not_queued'
 WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>$2 OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END THEN 'stale' ELSE e.status END status,
 CASE WHEN e.document_id IS NULL THEN 'not_queued'
 WHEN e.source_hash<>d.source_hash OR coalesce(e.requested_model,e.model)<>$2 OR e.analysis_version<>CASE WHEN d.document_kind IN('pdf','image') THEN $4 ELSE $3 END THEN 'stale_generation'
 WHEN e.status='failed' THEN 'generation_failed' WHEN e.status='running' THEN 'processing' WHEN e.status='pending' THEN 'queued' ELSE 'ready' END reason
 FROM knowledge.documents d LEFT JOIN knowledge.document_enrichments e ON e.document_id=d.id
 WHERE d.course_id=ANY($1::bigint[]) AND d.is_current=1 AND d.status='ready' AND d.document_kind<>'archive'
) `
