package knowledge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
)

type ListRequest struct {
	CourseID        int64    `json:"course_id"`
	PathPrefix      string   `json:"path_prefix"`
	DocumentKinds   []string `json:"document_kinds"`
	ChangedSince    string   `json:"changed_since"`
	IncludeInsights bool     `json:"include_insights"`
	Cursor          string   `json:"cursor"`
	Limit           int      `json:"limit"`
}
type PublicDocument struct {
	queries.KnowledgeDocument
	Error            *string        `json:"error,omitempty"`
	ResourceURI      string         `json:"resource_uri"`
	EvidenceClass    string         `json:"evidence_class"`
	UntrustedContent bool           `json:"untrusted_content"`
	StudyAnalysis    map[string]any `json:"study_analysis,omitempty"`
}
type MaterialList struct {
	Materials              []PublicDocument `json:"materials"`
	NextCursor             *string          `json:"next_cursor"`
	DerivedInsightNotice   *string          `json:"derived_insight_notice"`
	UntrustedContentNotice string           `json:"untrusted_content_notice"`
}

func (s Reader) Materials(ctx context.Context, request ListRequest) (MaterialList, error) {
	result := MaterialList{Materials: []PublicDocument{}, UntrustedContentNotice: UntrustedNotice}
	if _, err := s.Visible(ctx, []int64{request.CourseID}); err != nil {
		return result, err
	}
	prefix, kinds, since, limit, err := materialRequest(request)
	if err != nil {
		return result, err
	}
	rows, err := s.Pool.Query(
		ctx,
		`SELECT to_jsonb(d) FROM knowledge.documents d WHERE `+CurrentSourcePredicate+` AND course_id=$1 AND ($2='' OR id>$2) AND ($3='' OR starts_with(normalized_path,$3)) AND (cardinality($4::text[])=0 OR document_kind=ANY($4::text[])) AND ($5::timestamptz IS NULL OR indexed_at::timestamptz >= $5) ORDER BY id LIMIT $6`,
		request.CourseID,
		request.Cursor,
		prefix,
		kinds,
		since,
		limit+1,
	)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		stop, err := appendMaterial(rows, &result, limit)
		if err != nil {
			rows.Close()
			return result, err
		}
		if stop {
			break
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	if request.IncludeInsights {
		if err := s.attachStudyAnalyses(ctx, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func materialRequest(request ListRequest) (prefix string, kinds []string, since *time.Time, limit int, err error) {
	limit = min(max(1, request.Limit), 100)
	if request.Limit == 0 {
		limit = 50
	}
	kinds = request.DocumentKinds
	if kinds == nil {
		kinds = []string{}
	}
	if request.PathPrefix != "" {
		prefix = strings.TrimRight(identity.Path(request.PathPrefix), "/") + "/"
	}
	if request.ChangedSince != "" {
		value, parseErr := time.Parse(time.RFC3339, request.ChangedSince)
		if parseErr != nil {
			return "", nil, nil, 0, parseErr
		}
		since = &value
	}
	return prefix, kinds, since, limit, nil
}

func appendMaterial(rows pgx.Rows, result *MaterialList, limit int) (bool, error) {
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return false, err
	}
	var doc queries.KnowledgeDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false, err
	}
	if len(result.Materials) == limit {
		cursor := result.Materials[len(result.Materials)-1].ID
		result.NextCursor = &cursor
		return true, nil
	}
	for _, text := range []*string{
		&doc.CourseName,
		&doc.DisplayName,
		&doc.SourcePath,
		&doc.NormalizedPath,
		doc.SourceUrl,
		doc.CourseShortName,
	} {
		if text != nil {
			*text = identity.Decode(*text)
		}
	}
	evidence := "official_material"
	if doc.SourceOrigin == "external" {
		evidence = "course_material"
	}
	result.Materials = append(
		result.Materials,
		PublicDocument{
			KnowledgeDocument: doc,
			ResourceURI:       "eclass://documents/" + doc.ID,
			EvidenceClass:     evidence,
			UntrustedContent:  true,
		},
	)
	return false, nil
}

func (s Reader) attachStudyAnalyses(ctx context.Context, result *MaterialList) error {
	notice := DerivedNotice
	result.DerivedInsightNotice = &notice
	for i, doc := range result.Materials {
		analysis, err := s.documentAnalysis(ctx, doc.ID, doc.SourceHash)
		if err != nil {
			return err
		}
		result.Materials[i].StudyAnalysis = analysis
	}
	return nil
}
