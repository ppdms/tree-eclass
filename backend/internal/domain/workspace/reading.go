package workspace

import (
	"context"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
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
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return Reading{}, err
	}
	defer tx.Rollback(ctx)
	var id int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND (hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=app.courses.id AND p.enabled=1))`, course).Scan(&id); err != nil {
		return Reading{}, err
	}
	result, err := readingTotals(ctx, tx, course, action, document)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func readingTotals(ctx context.Context, tx rdbms.Tx, course int64, action, document string) (Reading, error) {
	result := Reading{Pages: []PageReading{}}
	rows, err := tx.Query(ctx, `SELECT document_id,page_number,sum(active_seconds)::bigint,sum(visible_seconds)::bigint
 FROM app.study_reading_spans WHERE course_id=$1 AND ($2='' OR action_id=$2) AND ($3='' OR document_id=$3)
 GROUP BY document_id,page_number ORDER BY document_id,page_number`, course, identity.Encode(action), document)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	previous := ""
	for rows.Next() {
		var page PageReading
		if err = rows.Scan(&page.Document, &page.Page, &page.Active, &page.Visible); err != nil {
			return result, err
		}
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
	return result, rows.Err()
}
