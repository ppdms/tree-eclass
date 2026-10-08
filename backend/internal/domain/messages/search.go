package messages

import (
	"context"
	"math"
	"sort"
	"time"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/rdbms"
)

type SearchRequest struct {
	Query   string  `json:"query"`
	Courses []int64 `json:"course_ids"`
	Limit   int     `json:"limit"`
	Mode    string  `json:"retrieval_mode"`
}
type Hit struct {
	Conversation
	First        string   `json:"first_message_id"`
	Last         string   `json:"last_message_id"`
	IDs          []string `json:"message_ids"`
	URLs         []string `json:"message_urls"`
	Count        int      `json:"message_count"`
	IDsTruncated bool     `json:"message_ids_truncated,omitempty"`
	Participants int64    `json:"participant_count"`
	Reactions    int64    `json:"reaction_count"`
	Pinned       bool     `json:"is_pinned"`
	Excerpt      string   `json:"excerpt"`
	Score        float64  `json:"retrieval_score"`
	Mode         string   `json:"retrieval_mode"`
	Lexical      *float64 `json:"lexical_score"`
	Semantic     *float64 `json:"semantic_score"`
	Freshness    float64  `json:"freshness_score"`
	SourceWeight float64  `json:"source_weight"`
	Quality      float64  `json:"quality_multiplier"`
	URL          *string  `json:"message_url"`
	Resource     string   `json:"resource_uri"`
	Year         *string  `json:"academic_year"`
	Untrusted    bool     `json:"untrusted_content"`
}
type candidateHit struct {
	Hit
	Epoch                     float64 `json:"ended_at_epoch"`
	Guild                     *int64  `json:"guild_id"`
	lexicalRank, semanticRank int
}
type SearchResult struct {
	Query     string  `json:"query"`
	Results   []Hit   `json:"results"`
	Limit     int     `json:"limit_applied"`
	Available bool    `json:"available"`
	Latest    *string `json:"archive_indexed_through"`
	Notice    string  `json:"untrusted_content_notice"`
}

func (s Reader) Search(ctx context.Context, request SearchRequest, now time.Time) (SearchResult, error) {
	check := knowledge.SearchRequest{
		Query:     request.Query,
		CourseIDs: request.Courses,
		Limit:     request.Limit,
		Mode:      request.Mode,
	}
	if err := check.Validate(); err != nil {
		return SearchResult{}, err
	}
	request.Query, request.Mode, request.Limit = check.Query, check.Mode, min(20, max(1, check.Limit))
	result := SearchResult{Query: request.Query, Results: []Hit{}, Limit: request.Limit, Notice: CommunityNotice}
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	ids, err := visible(ctx, tx, request.Courses)
	if err != nil {
		return result, err
	}
	status, err := statusTx(ctx, tx, ids)
	if err != nil {
		return result, err
	}
	result.Available, result.Latest = status.Available, status.Latest
	if !result.Available {
		return result, nil
	}
	limit := max(40, request.Limit*8)
	lexical, semantic := []candidateHit{}, []candidateHit{}
	if request.Mode != "semantic" {
		lexical, err = lexicalSearch(ctx, tx, ids, request.Query, limit)
		if err != nil {
			return result, err
		}
	}
	if request.Mode != "lexical" {
		semantic, err = semanticSearch(ctx, tx, ids, request.Query, limit, lexical)
		if err != nil {
			return result, err
		}
	}
	merged := rank(lexical, semantic, request.Mode, request.Query, now)
	for _, row := range merged[:min(request.Limit, len(merged))] {
		if err = decorateHit(ctx, tx, &row); err != nil {
			return result, err
		}
		result.Results = append(result.Results, row.Hit)
	}
	return result, tx.Commit(ctx)
}

func rank(lexical, semantic []candidateHit, mode, query string, now time.Time) []candidateHit {
	merged := map[string]candidateHit{}
	for i, c := range lexical {
		c.lexicalRank = i + 1
		merged[c.ID] = c
	}
	for i, c := range semantic {
		if old, ok := merged[c.ID]; ok {
			c.lexicalRank, c.Lexical, c.Excerpt = old.lexicalRank, old.Lexical, old.Excerpt
		}
		c.semanticRank = i + 1
		merged[c.ID] = c
	}
	result := []candidateHit{}
	half := 730.0
	if policyQuery(query) {
		half = 180
	}
	for _, c := range merged {
		base := 0.0
		if c.lexicalRank > 0 {
			weight := .55
			if mode == "lexical" {
				weight = 1
			}
			base += weight / float64(60+c.lexicalRank)
		}
		if c.semanticRank > 0 {
			weight := .45
			if mode == "semantic" {
				weight = 1
			}
			base += weight / float64(60+c.semanticRank)
		}
		freshness := math.Exp2(-max(0, float64(now.Unix())-c.Epoch) / 86400 / half)
		c.SourceWeight = .85
		if c.Pinned || reliableChannel(c.Name) {
			c.SourceWeight = .95
		}
		quality := 1 + min(.1, math.Log1p(float64(max(0, c.Reactions)))*.02+float64(min(3, max(0, c.Participants)))*.01)
		c.Score = base * (.70 + .60*freshness) * c.SourceWeight * quality
		c.Freshness, c.Quality, c.Mode = math.RoundToEven(freshness*1e6)/1e6, math.RoundToEven(quality*1e6)/1e6, mode
		result = append(result, c)
	}
	sort.SliceStable(result, func(i, j int) bool { return better(result[i], result[j]) })
	return result
}

func better(a, b candidateHit) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Epoch != b.Epoch {
		return a.Epoch > b.Epoch
	}
	return a.ID < b.ID
}

func decorateHit(ctx context.Context, tx rdbms.Tx, c *candidateHit) error {
	for _, value := range []*string{&c.CourseName, c.CourseShortName, &c.Name, &c.Excerpt} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	c.Source, c.Evidence, c.Untrusted = "discord", "community_discussion", true
	c.Resource = "discord://conversations/" + c.ID
	c.URL = messageURL(c.Guild, c.Channel, c.First)
	c.Metadata = map[string]any{"guild_id": c.Guild}
	c.Year = academicYear(c.Ended)
	rows, err := tx.Query(
		ctx,
		`SELECT message_id::text FROM messages.conversation_messages WHERE conversation_id=$1 ORDER BY position LIMIT 201`,
		c.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.IDs = ids
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM messages.conversation_messages WHERE conversation_id=$1`, c.ID).Scan(&c.Count); err != nil {
		return err
	}
	c.IDsTruncated = c.Count > 200
	c.IDs = c.IDs[:min(200, len(c.IDs))]
	c.URLs = []string{}
	for _, id := range c.IDs {
		if url := messageURL(c.Guild, c.Channel, id); url != nil {
			c.URLs = append(c.URLs, *url)
		}
	}
	return nil
}
