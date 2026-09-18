package knowledge

import (
	"container/heap"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"tree-eclass/internal/domain/identity"
)

var policyKeywords = []string{
	"intro",
	"introduction",
	"course",
	"description",
	"syllabus",
	"grading",
	"grade",
	"βαθμολογια",
	"περιγραφη",
	"εισαγωγη",
	"οργανωση",
}

func metadataRank(c *candidate, query string, semantic bool) {
	normalized, name, path := identity.Search(query), identity.Search(c.DisplayName), identity.Search(c.SourcePath)
	bonus := 0.0
	if strings.Contains(name, normalized) {
		bonus += 0.16
	}
	nameHits, pathHits := 0, 0
	for _, token := range strings.Fields(normalized) {
		if strings.Contains(name, token) {
			nameHits++
		}
		if strings.Contains(path, token) {
			pathHits++
		}
	}
	bonus += min(0.12, float64(nameHits)*0.04) + min(0.06, float64(pathHits)*0.02)
	c.DocumentPriority = "general_material"
	for _, keyword := range policyKeywords {
		if strings.Contains(name, keyword) || strings.Contains(path, keyword) {
			bonus += 0.08
			c.DocumentPriority = "course_policy"
			break
		}
	}
	if c.AcademicYear != nil && len(*c.AcademicYear) >= 4 && strings.HasPrefix(*c.AcademicYear, "20") {
		if year, err := strconv.Atoi((*c.AcademicYear)[:4]); err == nil {
			bonus += 0.04 / float64(1+max(0, time.Now().UTC().Year()-year))
		}
	}
	c.MetadataScore = math.Round(bonus*1e6) / 1e6
	if semantic {
		c.Score += bonus
	} else {
		c.Score -= bonus
	}
}
func before(a, b candidate, descending bool) bool {
	if a.Score != b.Score {
		if descending {
			return a.Score > b.Score
		}
		return a.Score < b.Score
	}
	if a.DocumentID != b.DocumentID {
		return a.DocumentID < b.DocumentID
	}
	return a.Ordinal < b.Ordinal
}

// Keep only the best bounded window while scanning packed embeddings. A larger
// corpus increases scan time, not application memory proportional to the corpus.
type candidates []candidate

func (c candidates) Len() int           { return len(c) }
func (c candidates) Less(i, j int) bool { return before(c[j], c[i], true) }
func (c candidates) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c *candidates) Push(x any)        { *c = append(*c, x.(candidate)) }
func (c *candidates) Pop() any {
	old := *c
	item := old[len(old)-1]
	*c = old[:len(old)-1]
	return item
}
func (c *candidates) admit(item candidate, limit int) {
	if len(*c) < limit {
		heap.Push(c, item)
	} else if before(item, (*c)[0], true) {
		(*c)[0] = item
		heap.Fix(c, 0)
	}
}

func fuse(lexical, semantic []candidate) []candidate {
	merged := map[string]candidate{}
	for i, c := range lexical {
		score := c.Score
		c.LexicalScore = &score
		c.Score = 0.55 / float64(61+i)
		merged[c.ID] = c
	}
	for i, c := range semantic {
		if old, exists := merged[c.ID]; exists {
			c.LexicalScore = old.LexicalScore
			c.Score = old.Score
			c.Excerpt = old.Excerpt
		} else {
			c.Score = 0
		}
		c.Score += 0.45 / float64(61+i)
		merged[c.ID] = c
	}
	result := make([]candidate, 0, len(merged))
	for _, c := range merged {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return before(result[i], result[j], true) })
	return result
}
