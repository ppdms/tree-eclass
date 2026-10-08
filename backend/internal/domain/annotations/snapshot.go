package annotations

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Snapshot reads marks in an existing snapshot without rewriting their anchors.
// Explicit reanchoring still records orphaning durably through List.
func Snapshot(
	ctx context.Context,
	tx rdbms.Tx,
	course int64,
	document, action string,
	deleted bool,
) ([]Annotation, error) {
	if document != "" {
		action = ""
	}
	rows, err := tx.Query(ctx, `SELECT `+annotationColumns+`,d.source_hash FROM app.study_annotations a
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
		item, hash, size, err := scanSnapshotRow(rows)
		if err != nil {
			return nil, err
		}
		bytes += size
		if len(result) >= 1000 || bytes > 8*1024*1024 {
			return nil, errors.New("workspace annotations exceed the bounded response budget")
		}
		if item.Status == "active" && (hash == nil || *hash != item.SourceHash) {
			item.Status = "orphaned"
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// scanSnapshotRow reads one explicit-column snapshot row plus the joined
// source hash, returning the decoded annotation, the hash, and the encoded
// size for the bounded response budget.
func scanSnapshotRow(rows rdbms.Rows) (Annotation, *string, int, error) {
	var id, courseID, page int64
	var document, hash, kind, origin, status, color string
	var quote, prefix, suffix, action, unit, revision string
	var start, end, session *int64
	var rects, tags string
	var chunk, body *string
	var created, updated string
	var source *string
	if err := rows.Scan(
		&id, &courseID, &document, &hash, &page, &kind, &origin, &status,
		&color, &quote, &prefix, &suffix, &start, &end, &rects, &chunk,
		&body, &tags, &action, &unit, &revision, &session, &created, &updated,
		&source,
	); err != nil {
		return Annotation{}, nil, 0, err
	}
	raw := assembleAnnotation(
		id, courseID, page, document, hash, kind, origin, status, color,
		quote, prefix, suffix, start, end, rects, chunk, body, tags,
		action, unit, revision, session, created, updated,
	)
	item, err := decode(raw)
	if err != nil {
		return Annotation{}, nil, 0, err
	}
	return item, source, len(raw), nil
}
