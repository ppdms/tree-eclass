package messages

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/rdbms"
)

const searchSources = ` FROM messages.conversations c
 JOIN app.courses co ON co.id=c.course_id AND co.hidden=0
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=c.root_id::text AND a.status='ready'
 LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id `

// hitColumns selects every field scanCandidateHit needs, in scan order, with an
// excerpt slot appended by each caller. CAST keeps integer/boolean values
// portable: database/sql Scan into *string fails on sqlite's INTEGER storage
// class, and both drivers expose the same TEXT affinity.
const hitColumns = `c.conversation_id,c.course_id,co.name course_name,co.short_name course_short_name,CAST(c.channel_id AS TEXT) channel_id,
 c.channel_name,c.channel_type,CAST(c.first_message_id AS TEXT) first_message_id,CAST(c.last_message_id AS TEXT) last_message_id,
 c.started_at,c.ended_at,c.ended_at_epoch,c.participant_count,c.reaction_count,CAST(c.is_pinned AS BIGINT) is_pinned,ch.guild_id`

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func likePattern(term string) string {
	return `%` + likeEscaper.Replace(term) + `%`
}

// scanCandidateHit scans one hitColumns row plus a trailing excerpt into c.
// extra, when non-nil, receives further trailing columns (e.g. the packed
// embedding vector in semanticSearch) via database/sql Scan.
func scanCandidateHit(rows rdbms.Rows, c *candidateHit, extra ...any) error {
	var shortName, guild any
	var pinned int64
	dest := []any{
		&c.ID, &c.CourseID, &c.CourseName, &shortName, &c.Channel,
		&c.Name, &c.Kind, &c.First, &c.Last,
		&c.Started, &c.Ended, &c.Epoch, &c.Participants, &c.Reactions, &pinned, &guild,
		&c.Excerpt,
	}
	dest = append(dest, extra...)
	if err := rows.Scan(dest...); err != nil {
		return err
	}
	if name, ok := shortName.(string); ok {
		c.CourseShortName = &name
	} else {
		c.CourseShortName = nil
	}
	c.Pinned = pinned != 0
	if id, ok := guild.(int64); ok {
		c.Guild = &id
	} else {
		c.Guild = nil
	}
	var score float64
	if c.Lexical != nil {
		score = *c.Lexical
	}
	c.Score = score
	return nil
}

// coursePlaceholders expands ids into ($base,...) placeholders, appending one
// argument per id to args.
func coursePlaceholders(ids []int64, args *[]any, base int) string {
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", base+i)
		*args = append(*args, id)
	}
	return strings.Join(placeholders, ",")
}

func lexicalSearch(ctx context.Context, tx rdbms.Tx, ids []int64, query string, limit int) ([]candidateHit, error) {
	terms := lexicalQuery(query)
	if len(terms) == 0 || len(ids) == 0 {
		return []candidateHit{}, nil
	}
	params := []any{}
	courses := coursePlaceholders(ids, &params, 1)
	clauses := make([]string, len(terms))
	for i, term := range terms {
		params = append(params, likePattern(term))
		clauses[i] = fmt.Sprintf("CAST(f.search_vector AS TEXT) LIKE $%d ESCAPE '\\'", len(ids)+1+i)
	}
	params = append(params, limit)
	rows, err := tx.Query(ctx, `SELECT `+hitColumns+`,substr(c.text,1,400) excerpt`+searchSources+`
 JOIN messages.conversations_fts f ON f.conversation_id=c.conversation_id
 WHERE c.course_id IN (`+courses+`) AND `+strings.Join(clauses, " AND ")+`
 ORDER BY c.ended_at_epoch DESC,c.conversation_id LIMIT $`+fmt.Sprint(len(params)), params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []candidateHit{}
	score := -float64(len(terms))
	for rows.Next() {
		var c candidateHit
		c.Lexical = &score
		if err = scanCandidateHit(rows, &c); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func semanticSearch(
	ctx context.Context,
	tx rdbms.Tx,
	ids []int64,
	query string,
	limit int,
	lexical []candidateHit,
) ([]candidateHit, error) {
	if len(ids) == 0 {
		return []candidateHit{}, nil
	}
	recentParams := []any{}
	recent := coursePlaceholders(ids, &recentParams, 1)
	recentParams = append(recentParams, 5000)
	offset := len(recentParams)
	selectedParams := []any{}
	selected := coursePlaceholders(ids, &selectedParams, offset+1)
	union, selectedParams := semanticCandidates(selectedParams, offset, lexical)
	rows, err := querySemanticCandidates(ctx, tx, recent, recentParams, selected, selectedParams, offset, union)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rankSemantic(rows, knowledge.Embed(query), limit)
}

// semanticCandidates builds the UNION of recent conversations and lexical
// hits that semantic ranking scores, appending the candidate id arguments
// and the embedding model to selectedParams.
func semanticCandidates(selectedParams []any, offset int, lexical []candidateHit) (string, []any) {
	seen := map[string]bool{}
	lexicalIDs := []string{}
	for _, c := range lexical {
		if !seen[c.ID] {
			seen[c.ID] = true
			lexicalIDs = append(lexicalIDs, c.ID)
		}
	}
	candidatePlaceholders := make([]string, len(lexicalIDs))
	for i, id := range lexicalIDs {
		candidatePlaceholders[i] = fmt.Sprintf("$%d", offset+len(selectedParams)+1+i)
		selectedParams = append(selectedParams, id)
	}
	selectedParams = append(selectedParams, knowledge.LocalEmbeddingModel)
	union := "SELECT conversation_id FROM recent"
	if len(candidatePlaceholders) > 0 {
		union += " UNION SELECT conversation_id FROM (SELECT " + strings.Join(candidatePlaceholders, " UNION ALL SELECT ") + ") candidates(conversation_id)"
	}
	return union, selectedParams
}

// semanticEmbeddingJoin restricts the candidate join to the current local
// embedding rows: 384 float32 dimensions packed as 1536 bytes.
const semanticEmbeddingJoin = ` JOIN messages.conversation_embeddings e ON e.conversation_id=c.conversation_id AND e.model=$`

// querySemanticCandidates loads the candidate rows with their packed
// embedding vectors for Go-side cosine ranking.
func querySemanticCandidates(
	ctx context.Context, tx rdbms.Tx, recent string, recentParams []any,
	selected string, selectedParams []any, offset int, union string,
) (rdbms.Rows, error) {
	return tx.Query(ctx, `WITH recent AS (SELECT c.conversation_id `+searchSources+`
 WHERE c.course_id IN (`+recent+`) ORDER BY c.ended_at_epoch DESC,c.conversation_id LIMIT $`+fmt.Sprint(offset)+`),
 candidates AS (`+union+`)
 SELECT `+hitColumns+`,substr(c.text,1,1200) excerpt,e.vector FROM candidates
 JOIN messages.conversations c ON c.conversation_id=candidates.conversation_id
 JOIN app.courses co ON co.id=c.course_id AND co.hidden=0
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=CAST(c.root_id AS TEXT) AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'
	LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id`+
		semanticEmbeddingJoin+fmt.Sprint(offset+len(selectedParams))+` AND e.dimensions=384 AND length(e.vector)=1536
 WHERE c.course_id IN (`+selected+`)`, append(recentParams, selectedParams...)...)
}

// rankSemantic scores candidate rows by cosine similarity to the query
// vector and keeps the best limit hits.
func rankSemantic(rows rdbms.Rows, queryVector []float64, limit int) ([]candidateHit, error) {
	best := &hitHeap{}
	for rows.Next() {
		var c candidateHit
		var vector []byte
		if err := scanCandidateHit(rows, &c, &vector); err != nil {
			return nil, err
		}
		c.Score = knowledge.CosinePacked(queryVector, vector)
		if math.IsNaN(c.Score) || math.IsInf(c.Score, 0) {
			continue
		}
		score := c.Score
		c.Semantic = &score
		if best.Len() < limit {
			heap.Push(best, c)
		} else if better(c, (*best)[0]) {
			(*best)[0] = c
			heap.Fix(best, 0)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := []candidateHit(*best)
	sort.Slice(result, func(i, j int) bool { return better(result[i], result[j]) })
	return result, nil
}

type hitHeap []candidateHit

func (h hitHeap) Len() int           { return len(h) }
func (h hitHeap) Less(i, j int) bool { return better(h[j], h[i]) }
func (h hitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hitHeap) Push(v any)        { *h = append(*h, v.(candidateHit)) }
func (h *hitHeap) Pop() any          { old := *h; last := old[len(old)-1]; *h = old[:len(old)-1]; return last }
