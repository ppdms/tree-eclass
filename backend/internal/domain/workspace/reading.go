package workspace

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type PageReading struct {
	Document string `json:"document_id"`
	Page     int64  `json:"page_number"`
	Active   int64  `json:"active_seconds"`
	Visible  int64  `json:"visible_seconds"`
}
type Reading struct {
	Active    int64         `json:"active_seconds"`
	Visible   int64         `json:"visible_seconds"`
	Minutes   int64         `json:"active_minutes"`
	PagesRead int64         `json:"pages_read"`
	Documents int64         `json:"documents_read"`
	Pages     []PageReading `json:"pages"`
}

func (s Service) Reading(ctx context.Context, course int64, action, document string) (Reading, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return Reading{}, err
	}
	defer tx.Rollback(ctx)
	if err = tx.Workspace().CheckVisibleCourse(ctx, course); err != nil {
		return Reading{}, err
	}
	result, err := readingTotals(ctx, tx, course, action, document)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func readingTotals(
	ctx context.Context, tx database.Operations, course int64, action, document string,
) (Reading, error) {
	result := Reading{Pages: []PageReading{}}
	pages, err := tx.Workspace().ReadingTotals(ctx, course, identity.Encode(action), document)
	if err != nil {
		return result, err
	}
	previous := ""
	for _, row := range pages {
		page := PageReading{Document: row.Document, Page: row.Page, Active: row.Active, Visible: row.Visible}
		if previous != page.Document {
			previous = page.Document
			result.Documents++
		}
		result.Active += page.Active
		result.Visible += page.Visible
		result.Pages = append(result.Pages, page)
	}
	result.PagesRead = int64(len(result.Pages))
	result.Minutes = result.Active / 60
	return result, nil
}
