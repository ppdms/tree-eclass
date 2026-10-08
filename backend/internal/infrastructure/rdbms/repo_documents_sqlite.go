package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteDocuments struct{ db nativeDBTX }

func sqliteCourseArgs(courses []int64) []any {
	args := make([]any, 0, len(courses))
	for _, id := range courses {
		args = append(args, id)
	}
	return args
}

// searchScopeSQLite builds the admitted-document filter shared by search paths.
func searchScopeSQLite(filter database.DocumentFilter, args []any) (string, []any) {
	scope := documentAdmissionSQLite + ` AND d.status='ready' AND d.course_id IN` +
		sqlitePlaceholders(len(filter.CourseIDs))
	for _, id := range filter.CourseIDs {
		args = append(args, id)
	}
	if len(filter.DocumentKinds) > 0 {
		scope += ` AND d.document_kind IN` + sqlitePlaceholders(len(filter.DocumentKinds))
		for _, kind := range filter.DocumentKinds {
			args = append(args, kind)
		}
	}
	scope += ` AND (?='' OR substr(d.normalized_path,1,length(CAST(? AS TEXT)))=CAST(? AS TEXT))`
	return scope, append(args, filter.FolderPrefix, filter.FolderPrefix, filter.FolderPrefix)
}

func (d sqliteDocuments) VisibleCourseIDs(ctx context.Context) ([]int64, error) {
	rows, err := d.db.Query(ctx, `SELECT id FROM courses WHERE hidden=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (d sqliteDocuments) CourseVisible(ctx context.Context, course int64) (bool, error) {
	var visible int64
	err := d.db.QueryRow(ctx, `SELECT 1 FROM courses WHERE id=? AND hidden=0`, course).Scan(&visible)
	if err != nil {
		return false, err
	}
	return visible == 1, nil
}
func (d sqliteDocuments) DocumentAdmitted(ctx context.Context, id string) (bool, error) {
	var admitted int64
	err := d.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents d WHERE d.id=? AND `+
		documentAdmissionSQLite+`)`, id).Scan(&admitted)
	return admitted == 1, err
}
func (d sqliteDocuments) GetReadableDocument(ctx context.Context, id string) (database.KnowledgeDocument, error) {
	var doc database.KnowledgeDocument
	err := d.db.QueryRow(ctx, `SELECT `+documentColumnsSQLite+` FROM documents d`+
		` JOIN courses c ON c.id=d.course_id AND c.hidden=0`+
		` WHERE d.id=? AND d.is_current=1 AND `+documentAdmissionSQLite, id).
		Scan(scanDocumentPointers(&doc)...)
	return doc, err
}

func (d sqliteDocuments) ListMaterials(ctx context.Context, course int64, cursor, prefix, kind string,
	since *string, limit int) ([]database.KnowledgeDocument, error) {
	query := `SELECT ` + documentColumnsSQLite + ` FROM documents d` +
		` WHERE ` + documentAdmissionSQLite + ` AND d.course_id=? AND (?='' OR d.id>?)` +
		` AND (?='' OR substr(d.normalized_path,1,length(CAST(? AS TEXT)))=CAST(? AS TEXT))` +
		` AND (?='' OR d.document_kind=?) ORDER BY d.id`
	args := []any{course, cursor, cursor, prefix, prefix, prefix, kind, kind}
	if since == nil {
		query += ` LIMIT ?`
		args = append(args, limit+1)
	}
	return listMaterialRows(ctx, d.db, query, args, since, limit)
}

func (d sqliteDocuments) ListAdminDocuments(ctx context.Context, courses []int64, status, query string,
	limit int) ([]database.AdminDocument, error) {
	args := append(sqliteCourseArgs(courses), status, status)
	statement := `SELECT ` + documentColumnsSQLite +
		`,(SELECT count(*) FROM chunks c WHERE c.document_id=d.id)` +
		`,(SELECT count(DISTINCT c.id) FROM chunks c` +
		` JOIN chunk_embeddings e ON e.chunk_id=c.id WHERE c.document_id=d.id)` +
		` FROM documents d WHERE d.course_id IN` + sqlitePlaceholders(len(courses)) +
		` AND (?='' OR d.status=?)` +
		` ORDER BY coalesce(d.indexed_at,'') DESC,d.source_path COLLATE BINARY`
	if query == "" {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	return listAdminDocumentRows(ctx, d.db, statement, args, query, limit)
}
