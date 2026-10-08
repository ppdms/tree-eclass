package messages

import (
	"container/heap"
	"context"
	"math"
	"sort"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/knowledge"
)

func lexicalSearch(ctx context.Context, tx database.Tx, ids []int64, query string, limit int) ([]candidateHit, error) {
	terms := lexicalQuery(query)
	if len(terms) == 0 || len(ids) == 0 {
		return []candidateHit{}, nil
	}
	rows, err := tx.Community().LexicalCandidates(ctx, ids, terms, limit)
	if err != nil {
		return nil, err
	}
	result := []candidateHit{}
	score := -float64(len(terms))
	for _, row := range rows {
		c := assembleCandidate(row)
		c.Lexical = &score
		c.Score = score
		result = append(result, c)
	}
	return result, nil
}

func semanticSearch(
	ctx context.Context,
	tx database.Tx,
	ids []int64,
	query string,
	limit int,
	lexical []candidateHit,
) ([]candidateHit, error) {
	if len(ids) == 0 {
		return []candidateHit{}, nil
	}
	lexicalIDs := make([]string, 0, len(lexical))
	for _, c := range lexical {
		lexicalIDs = append(lexicalIDs, c.ID)
	}
	stream, err := tx.Community().SemanticCandidates(ctx, ids, lexicalIDs,
		knowledge.LocalEmbeddingModel, knowledge.EmbeddingDimensions)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	return rankSemantic(stream, knowledge.Embed(query), limit)
}

// rankSemantic scores streamed candidate rows by cosine similarity to the
// query vector and keeps the best limit hits.
func rankSemantic(stream database.Iterator[database.CommunityEmbeddedCandidate],
	queryVector []float64, limit int) ([]candidateHit, error) {
	best := &hitHeap{}
	for stream.Next() {
		row := stream.Value()
		c := assembleCandidate(row.CommunityCandidate)
		c.Score = knowledge.CosinePacked(queryVector, row.Vector)
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
	if err := stream.Err(); err != nil {
		return nil, err
	}
	result := []candidateHit(*best)
	sort.Slice(result, func(i, j int) bool { return better(result[i], result[j]) })
	return result, nil
}

func assembleCandidate(row database.CommunityCandidate) candidateHit {
	c := candidateHit{}
	c.ID, c.CourseID = row.ConversationID, row.CourseID
	c.CourseName, c.CourseShortName = row.CourseName, row.CourseShortName
	c.Channel = row.ChannelID
	c.Name, c.Kind = row.ChannelName, row.ChannelType
	c.First, c.Last = row.FirstMessageID, row.LastMessageID
	c.Started, c.Ended = row.StartedAt, row.EndedAt
	c.Epoch = row.EndedAtEpoch
	c.Participants, c.Reactions = row.Participants, row.Reactions
	c.Pinned = row.Pinned
	c.Guild = row.GuildID
	c.Excerpt = row.Excerpt
	return c
}

type hitHeap []candidateHit

func (h hitHeap) Len() int           { return len(h) }
func (h hitHeap) Less(i, j int) bool { return better(h[j], h[i]) }
func (h hitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hitHeap) Push(v any)        { *h = append(*h, v.(candidateHit)) }
func (h *hitHeap) Pop() any          { old := *h; last := old[len(old)-1]; *h = old[:len(old)-1]; return last }
