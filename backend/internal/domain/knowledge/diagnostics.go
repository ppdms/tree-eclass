package knowledge

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Coverage struct {
	CourseID           int64 `json:"course_id"`
	SupportedDocuments int64 `json:"supported_documents"`
	IndexedDocuments   int64 `json:"indexed_documents"`
	FailedDocuments    int64 `json:"failed_documents"`
	PendingDocuments   int64 `json:"pending_documents"`
}

func coverageFromRow(row database.CoverageRow) Coverage {
	return Coverage{
		CourseID: row.CourseID, SupportedDocuments: row.Supported,
		IndexedDocuments: row.Indexed, FailedDocuments: row.Failed, PendingDocuments: row.Pending,
	}
}

func (s Reader) Summary(ctx context.Context, course *int64) ([]Coverage, error) {
	rows, err := s.Pool.Documents().Coverage(ctx, course)
	if err != nil {
		return nil, err
	}
	result := []Coverage{}
	for _, row := range rows {
		result = append(result, coverageFromRow(row))
	}
	return result, nil
}

type AdminDocument struct {
	database.KnowledgeDocument
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
	rows, err := s.Pool.Documents().ListAdminDocuments(ctx, ids, status, identity.Encode(query),
		min(max(1, limit), 500))
	if err != nil {
		return nil, err
	}
	result := []AdminDocument{}
	for _, row := range rows {
		item := AdminDocument{KnowledgeDocument: row.KnowledgeDocument,
			ChunkCount: row.ChunkCount, EmbeddingCount: row.EmbeddingCount}
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
	return result, nil
}
