package knowledge

import (
	"context"
	"encoding/json"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
)

type Coverage struct {
	CourseID           int64 `json:"course_id"`
	SupportedDocuments int64 `json:"supported_documents"`
	IndexedDocuments   int64 `json:"indexed_documents"`
	FailedDocuments    int64 `json:"failed_documents"`
	PendingDocuments   int64 `json:"pending_documents"`
}

func (s Reader) Summary(ctx context.Context, course *int64) ([]Coverage, error) {
	rows, err := s.Pool.Query(
		ctx,
		`SELECT c.id,count(d.id) FILTER(WHERE d.status NOT IN('unsupported','external')),count(d.id) FILTER(WHERE d.status='ready'),count(d.id) FILTER(WHERE d.status IN('failed','skipped_limit')),count(d.id) FILTER(WHERE d.status IN('pending','running')) FROM app.courses c LEFT JOIN knowledge.documents d ON d.course_id=c.id AND d.is_current=1 WHERE c.hidden=0 AND ($1::bigint IS NULL OR c.id=$1) GROUP BY c.id ORDER BY c.id`,
		course,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Coverage{}
	for rows.Next() {
		var c Coverage
		if err = rows.Scan(
			&c.CourseID,
			&c.SupportedDocuments,
			&c.IndexedDocuments,
			&c.FailedDocuments,
			&c.PendingDocuments,
		); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

type AdminDocument struct {
	queries.KnowledgeDocument
	ChunkCount     int64 `json:"chunk_count"`
	EmbeddingCount int64 `json:"embedding_count"`
}

func (s Reader) Documents(
	ctx context.Context,
	course *int64,
	status, query string,
	limit int,
) ([]AdminDocument, error) {
	var requested []int64
	if course != nil {
		requested = []int64{*course}
	}
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(
		ctx,
		`WITH documents AS MATERIALIZED(SELECT * FROM knowledge.documents WHERE course_id=ANY($1::bigint[]) AND ($2='' OR status=$2) AND ($3='' OR strpos(lower(display_name),lower($3))>0 OR strpos(lower(source_path),lower($3))>0) ORDER BY coalesce(indexed_at,'') DESC,source_path LIMIT $4)
 SELECT to_jsonb(d)||jsonb_build_object('chunk_count',(SELECT count(*) FROM knowledge.chunks c WHERE c.document_id=d.id),'embedding_count',(SELECT count(DISTINCT c.id) FROM knowledge.chunks c JOIN knowledge.chunk_embeddings e ON e.chunk_id=c.id WHERE c.document_id=d.id)) FROM documents d`,
		ids,
		status,
		identity.Encode(query),
		min(max(1, limit), 500),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []AdminDocument{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item AdminDocument
		if err = json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		for _, text := range []*string{
			&item.CourseName,
			&item.DisplayName,
			&item.SourcePath,
			&item.NormalizedPath,
			item.CourseShortName,
			item.SourceUrl,
			item.Error,
		} {
			if text != nil {
				*text = identity.Decode(*text)
			}
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
