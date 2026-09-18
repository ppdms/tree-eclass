package messages

import (
	"container/heap"
	"context"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/knowledge"
)

const searchSources = ` FROM messages.conversations c
 JOIN app.courses co ON co.id=c.course_id AND co.hidden=0
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=c.root_id::text AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=c.root_id::text AND a.status='ready'
 LEFT JOIN messages.channels ch ON ch.channel_id=c.channel_id AND ch.course_id=c.course_id `

const hitFields = `c.conversation_id,c.course_id,co.name course_name,co.short_name course_short_name,c.channel_id::text channel_id,c.channel_name,c.channel_type,
 c.first_message_id::text first_message_id,c.last_message_id::text last_message_id,c.started_at,c.ended_at,c.ended_at_epoch,
 c.participant_count,c.reaction_count,c.is_pinned<>0 is_pinned,ch.guild_id`

func lexicalSearch(ctx context.Context, tx pgx.Tx, ids []int64, query string, limit int) ([]candidateHit, error) {
	match := lexicalQuery(query)
	if match == "" {
		return []candidateHit{}, nil
	}
	rows, err := tx.Query(ctx, `WITH q AS(SELECT public.tree_query($2) query)
 SELECT to_jsonb(v) FROM(SELECT `+hitFields+`,
 -ts_rank_cd(f.search_vector,q.query) lexical_score,
 left(ts_headline('public.tree_search',f.text,q.query,'StartSel=[, StopSel=], MaxWords=40, MinWords=15'),2000) excerpt
 `+searchSources+` JOIN messages.conversations_fts f ON f.conversation_id=c.conversation_id CROSS JOIN q
 WHERE c.course_id=ANY($1::bigint[]) AND f.search_vector @@ q.query
 ORDER BY lexical_score,c.ended_at_epoch DESC,c.conversation_id LIMIT $3) v`, ids, match, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []candidateHit{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var c candidateHit
		if err = decode(raw, &c); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func semanticSearch(
	ctx context.Context,
	tx pgx.Tx,
	ids []int64,
	query string,
	limit int,
	lexical []candidateHit,
) ([]candidateHit, error) {
	lexicalIDs := []string{}
	for _, c := range lexical {
		lexicalIDs = append(lexicalIDs, c.ID)
	}
	rows, err := tx.Query(ctx, `WITH recent AS MATERIALIZED(SELECT c.conversation_id `+searchSources+`
 WHERE c.course_id=ANY($1::bigint[]) ORDER BY c.ended_at_epoch DESC,c.conversation_id LIMIT 5000),
 candidates AS(SELECT conversation_id FROM recent UNION SELECT unnest($2::text[]))
 SELECT to_jsonb(v),e.vector FROM(SELECT `+hitFields+`,left(c.text,1200) excerpt
 `+searchSources+` JOIN candidates selected ON selected.conversation_id=c.conversation_id
 WHERE c.course_id=ANY($1::bigint[])) v
 JOIN messages.conversation_embeddings e ON e.conversation_id=v.conversation_id AND e.model=$3 AND e.dimensions=384 AND octet_length(e.vector)=1536`, ids, lexicalIDs, knowledge.LocalEmbeddingModel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	queryVector := knowledge.Embed(query)
	best := &hitHeap{}
	for rows.Next() {
		var raw, vector []byte
		if err = rows.Scan(&raw, &vector); err != nil {
			return nil, err
		}
		var c candidateHit
		if err = decode(raw, &c); err != nil {
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
	if err = rows.Err(); err != nil {
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
