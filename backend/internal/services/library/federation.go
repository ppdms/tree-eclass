package library

import (
	"context"
	"strconv"
	"time"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/messages"
)

func (r *Registry) federate(ctx context.Context, in federationInput) (any, error) {
	official, err := (knowledge.Reader{Pool: r.Pool}).Search(
		ctx,
		knowledge.SearchRequest{Query: in.Query, CourseIDs: in.IDs, Limit: in.Limit, Mode: in.Mode},
	)
	if err != nil {
		return nil, err
	}
	community, err := (messages.Reader{Pool: r.Pool}).Search(
		ctx,
		messages.SearchRequest{Query: in.Query, Courses: in.IDs, Limit: in.Limit, Mode: in.Mode},
		time.Now(),
	)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"query":                              official.Query,
		"official_materials":                 official.Results,
		"community_messages":                 community.Results,
		"official_result_count":              len(official.Results),
		"community_result_count":             len(community.Results),
		"archive_indexed_through":            community.Latest,
		"evidence_guidance":                  "Prefer directly relevant official eClass evidence. When official material does not answer the question, report dated Discord evidence as community discussion, corroborate across messages where possible, and surface contradictions.",
		"official_untrusted_content_notice":  knowledge.UntrustedNotice,
		"community_untrusted_content_notice": messages.CommunityNotice,
	}, nil
}

func (r *Registry) courses(ctx context.Context) (any, error) {
	list, err := (courses.Service{Pool: r.Pool}).List(ctx, false)
	if err != nil {
		return nil, err
	}
	coverage, err := (knowledge.Reader{Pool: r.Pool}).Summary(ctx, nil)
	if err != nil {
		return nil, err
	}
	byCourse := map[int64]knowledge.Coverage{}
	for _, c := range coverage {
		byCourse[c.CourseID] = c
	}
	result := []map[string]any{}
	for _, c := range list {
		count := byCourse[c.ID]
		result = append(
			result,
			map[string]any{
				"course_id":           c.ID,
				"name":                c.Name,
				"short_name":          c.ShortName,
				"indexed_documents":   count.IndexedDocuments,
				"supported_documents": count.SupportedDocuments,
				"pending_documents":   count.PendingDocuments,
				"failed_documents":    count.FailedDocuments,
				"resource_uri":        "eclass://courses/" + strconv.FormatInt(c.ID, 10),
			},
		)
	}
	return map[string]any{"courses": result, "untrusted_content_notice": knowledge.UntrustedNotice}, nil
}
