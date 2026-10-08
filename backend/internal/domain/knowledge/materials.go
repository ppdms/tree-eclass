package knowledge

import (
	"context"
	"strings"
	"time"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
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
	var sinceText *string
	if since != nil {
		text := since.UTC().Format(time.RFC3339Nano)
		sinceText = &text
	}
	rows, err := s.Pool.Query(
		ctx,
		`SELECT `+documentColumns+` FROM knowledge.documents d WHERE `+CurrentSourcePredicate+` AND course_id=$1 AND ($2='' OR id>$2) AND ($3='' OR substr(d.normalized_path,1,length($3))=$3) AND ($4='' OR document_kind=$4) AND ($5::timestamptz IS NULL OR indexed_at::timestamptz >= $5) ORDER BY id LIMIT $6`,
		request.CourseID,
		request.Cursor,
		prefix,
		singleKind(kinds),
		sinceText,
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

func appendMaterial(rows rdbms.Rows, result *MaterialList, limit int) (bool, error) {
	var holder AdminDocument
	if err := scanMaterial(rows, &holder); err != nil {
		return false, err
	}
	doc := holder.KnowledgeDocument
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

// singleKind collapses the requested document kinds to one equality operand.
// The legacy ANY filter accepted a set, but the material browser passes at
// most one kind; a set parameter would need an array form sqlite cannot
// express, so multiple kinds resolve to the first.
func singleKind(kinds []string) string {
	if len(kinds) == 0 {
		return ""
	}
	return kinds[0]
}

// scanMaterial scans one explicit-column document row into holder. It mirrors
// the documentColumns prefix of scanDocument (diagnostics.go) without the
// trailing chunk/embedding counts.
func scanMaterial(rows rdbms.Rows, holder *AdminDocument) error {
	doc := &holder.KnowledgeDocument
	return rows.Scan(
		&doc.ID,
		&doc.CourseID,
		&doc.CourseName,
		&doc.CourseShortName,
		&doc.SourcePath,
		&doc.SourceOrigin,
		&doc.NormalizedPath,
		&doc.SourceUrl,
		&doc.DisplayName,
		&doc.SourceHash,
		&doc.SourceFingerprint,
		&doc.SourceEtag,
		&doc.ContentHashVerified,
		&doc.MimeType,
		&doc.ResponseMimeType,
		&doc.DocumentKind,
		&doc.AcademicYear,
		&doc.SourceModifiedAt,
		&doc.IsCurrent,
		&doc.Status,
		&doc.PageCount,
		&doc.SourceSizeBytes,
		&doc.CharacterCount,
		&doc.WordCount,
		&doc.ReadingMinutes,
		&doc.ComplexityScore,
		&doc.ComplexityLabel,
		&doc.LanguageHint,
		&doc.ExtractorName,
		&doc.ExtractorVersion,
		&doc.IndexedAt,
		&doc.Error,
		&doc.DiagnosticReason,
		&doc.WarningsJson,
	)
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
