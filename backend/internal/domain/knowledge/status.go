package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	for key, select_ := range statusQueries {
		rows, err := statusRows(ctx, tx, select_, ids)
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

// statusQueries select explicit columns; statusRows assembles the JSON-shaped
// maps in Go so the queries stay portable: to_jsonb(row) has no sqlite form
// and fails at prepare time. Counts aggregate with count(CASE...) so both
// drivers compute the same values (FILTER has no sqlite support). Course
// filters keep =ANY($N) array parameters, which the sqlite driver expands to
// IN lists; payload lookups keep ->>, which both engines evaluate.
var statusQueries = map[string]statusSelect{
	"coverage":              {cols: []string{"course_id", "supported_documents", "indexed_documents", "failed_documents", "pending_documents"}, query: `SELECT c.id course_id, count(CASE WHEN d.status NOT IN('unsupported','external') THEN 1 END) supported_documents, count(CASE WHEN d.status='ready' THEN 1 END) indexed_documents, count(CASE WHEN d.status IN('failed','skipped_limit') THEN 1 END) failed_documents, count(CASE WHEN d.status IN('pending','running') THEN 1 END) pending_documents FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1 WHERE c.id=ANY($1::bigint[]) GROUP BY c.id ORDER BY c.id`},
	"documents":             {cols: []string{"status", "count"}, query: `SELECT status,count(*) count FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 GROUP BY status ORDER BY status`},
	"jobs":                  {cols: []string{"status", "count"}, query: `SELECT q.status,count(*) count FROM app.control_commands q JOIN knowledge.documents d ON d.id=q.payload->>'document_id' WHERE q.queue='index' AND d.course_id=ANY($1::bigint[]) AND d.is_current=1 GROUP BY q.status ORDER BY q.status`},
	"failed_documents":      {cols: []string{"document_id", "course_id", "display_name", "source_path", "status", "diagnostic_reason", "error"}, query: `SELECT id document_id,course_id,display_name,source_path,status,diagnostic_reason,left(error,1000) error FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 AND status='failed' ORDER BY course_id,source_path LIMIT 501`},
	"unsupported_documents": {cols: []string{"document_id", "course_id", "display_name", "source_path", "status", "diagnostic_reason", "error"}, query: `SELECT id document_id,course_id,display_name,source_path,status,diagnostic_reason,left(error,1000) error FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND is_current=1 AND status IN('unsupported','skipped_limit') ORDER BY course_id,source_path LIMIT 501`},
}

// statusSelect pairs one status query with its column names so statusRows can
// assemble the JSON-shaped maps the old to_jsonb(v) rows carried.
type statusSelect struct {
	cols  []string
	query string
}

// statusRows runs one statusSelect and assembles the JSON-shaped maps the old
// to_jsonb(v) rows carried. Text arrives encoded (identity.Encode) and decodes
// here; numbers decode from TEXT columns back to json.Number so downstream
// consumers see the same numeric values as before.
func statusRows(ctx context.Context, tx rdbms.Tx, select_ statusSelect, args ...any) ([]map[string]any, error) {
	rows, err := tx.Query(ctx, select_.query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		raw := make([]any, len(select_.cols))
		pointers := make([]any, len(select_.cols))
		for i := range raw {
			pointers[i] = &raw[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			return nil, err
		}
		item, err := decodeStatusRow(select_.cols, raw)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// decodeStatusRow converts one scanned row to its JSON shape. NULL text stays
// absent (nil) the way to_jsonb nulls did; numbers surface as json.Number so
// both drivers agree regardless of how the driver types COUNT results.
func decodeStatusRow(cols []string, raw []any) (map[string]any, error) {
	item := make(map[string]any, len(cols))
	for i, key := range cols {
		// pgx returns COUNT as int64 but literals and small expressions as
		// int32/int; modernc returns int64 for both. Normalize every integer
		// width to json.Number so both drivers agree.
		switch value := raw[i].(type) {
		case nil:
			item[key] = nil
		case int64:
			item[key] = statusNumber(key, value != 0, strconv.FormatInt(value, 10))
		case int32:
			item[key] = statusNumber(key, value != 0, strconv.FormatInt(int64(value), 10))
		case int:
			item[key] = statusNumber(key, value != 0, strconv.Itoa(value))
		case float64:
			item[key] = json.Number(strconv.FormatFloat(value, 'g', -1, 64))
		case bool:
			if key == "untrusted_content" {
				item[key] = value
				continue
			}
			if value {
				item[key] = json.Number("1")
			} else {
				item[key] = json.Number("0")
			}
		case []byte:
			text := string(value)
			if key == "count" || strings.HasSuffix(key, "_documents") || key == "course_id" {
				item[key] = json.Number(text)
				continue
			}
			item[key] = identity.Decode(text)
		case string:
			if key == "count" || strings.HasSuffix(key, "_documents") || key == "course_id" {
				item[key] = json.Number(value)
				continue
			}
			item[key] = identity.Decode(value)
		default:
			return nil, fmt.Errorf("status: unexpected %s type %T", key, raw[i])
		}
	}
	return item, nil
}

// statusNumber renders integer columns: the boolean-shaped untrusted_content
// flag becomes a real bool, everything else a json.Number (counts and ids).
func statusNumber(key string, nonzero bool, text string) any {
	if key == "untrusted_content" {
		return nonzero
	}
	return json.Number(text)
}

func embeddingStatus(ctx context.Context, tx rdbms.Tx, ids []int64) (map[string]any, error) {
	var chunks, embedded int64
	err := tx.QueryRow(ctx, `SELECT count(*),count(CASE WHEN EXISTS(SELECT 1 FROM knowledge.chunk_embeddings e WHERE e.chunk_id=c.id AND e.model=$2) THEN 1 END) FROM knowledge.chunks c JOIN knowledge.documents d ON d.id=c.document_id WHERE d.course_id=ANY($1::bigint[]) AND d.is_current=1 AND d.status='ready'`, ids, LocalEmbeddingModel).
		Scan(&chunks, &embedded)
	return map[string]any{
		"model":           LocalEmbeddingModel,
		"dimensions":      EmbeddingDimensions,
		"chunks":          chunks,
		"embedded_chunks": embedded,
		"missing_chunks":  chunks - embedded,
	}, err
}

func guideStatus(ctx context.Context, tx rdbms.Tx, ids []int64, a settings.AI, result map[string]any) error {
	args := []any{ids, a.Model, settings.DocumentAnalysisVersion, settings.PageSynthesisVersion}
	counts, err := statusRows(
		ctx,
		tx,
		statusSelect{cols: []string{"status", "count"}, query: guideStatusCTE + `SELECT status,count(*) count FROM guides GROUP BY status ORDER BY status`},
		args...)
	if err != nil {
		return err
	}
	rows, err := statusRows(
		ctx,
		tx,
		statusSelect{cols: []string{"document_id", "course_id", "display_name", "source_path", "model", "error", "status", "reason"}, query: guideStatusCTE + `SELECT document_id,course_id,display_name,source_path,model,error,status,reason FROM guides WHERE status<>'ready' ORDER BY course_id,source_path LIMIT 501`},
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
