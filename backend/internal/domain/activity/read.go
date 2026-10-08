package activity

import (
	"context"
	"encoding/json"
	"errors"

	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Reader struct{ Pool rdbms.Pool }

func (r Reader) Page(ctx context.Context, limit, offset int64, timeline bool) (Page, error) {
	limit = min(50, max(10, limit))
	offset = max(0, offset)
	result := Page{Groups: []Group{}, Next: offset}
	if offset > 1<<31-1 {
		return result, errors.New("activity offset exceeds the supported range")
	}
	rows, err := queries.ForPool(r.Pool).
		ActivityPage(ctx, queries.ActivityPageParams{Limit: int32(limit + 1), Offset: int32(offset)})
	if err != nil {
		return result, err
	}
	result.More = int64(len(rows)) > limit
	items := make([]Item, 0, min(len(rows), int(limit)))
	for _, raw := range rows[:min(len(rows), int(limit))] {
		var item Item
		if err = json.Unmarshal(raw, &item); err != nil {
			return result, err
		}
		item.decode()
		items = append(items, item)
	}
	result.Groups = Groups(items)
	result.Next += int64(len(items))
	if timeline {
		result.Timeline = &items
	}
	return result, nil
}
