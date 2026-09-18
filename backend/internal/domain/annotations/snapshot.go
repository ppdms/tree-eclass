package annotations

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

// Snapshot reads marks in an existing snapshot without rewriting their anchors.
// Explicit reanchoring still records orphaning durably through List.
func Snapshot(
	ctx context.Context,
	tx pgx.Tx,
	course int64,
	document, action string,
	deleted bool,
) ([]Annotation, error) {
	if document != "" {
		action = ""
	}
	rows, err := tx.Query(ctx, `SELECT `+annotationJSON+`,d.source_hash FROM app.study_annotations a
 LEFT JOIN knowledge.documents d ON d.id=a.document_id AND d.course_id=a.course_id AND d.is_current=1
 WHERE a.course_id=$1 AND ($2='' OR a.document_id=$2) AND ($3='' OR a.action_id=$3) AND ($4 OR a.status!='deleted')
 ORDER BY a.document_id,a.page_number,a.id LIMIT 1001`, course, document, identity.Encode(action), deleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Annotation{}
	bytes := 0
	for rows.Next() {
		var raw []byte
		var hash *string
		if err = rows.Scan(&raw, &hash); err != nil {
			return nil, err
		}
		bytes += len(raw)
		if len(result) >= 1000 || bytes > 8*1024*1024 {
			return nil, errors.New("workspace annotations exceed the bounded response budget")
		}
		item, err := decode(raw)
		if err != nil {
			return nil, err
		}
		if item.Status == "active" && (hash == nil || *hash != item.SourceHash) {
			item.Status = "orphaned"
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
