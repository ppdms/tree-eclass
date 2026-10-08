package rdbms

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/database"
)

type postgresDocuments struct{ db nativeDBTX }

// scanDocumentPointers returns scan targets for a full document row.
func scanDocumentPointers(doc *database.KnowledgeDocument) []any {
	return []any{
		&doc.ID, &doc.CourseID, &doc.CourseName, &doc.CourseShortName, &doc.SourcePath,
		&doc.SourceOrigin, &doc.NormalizedPath, &doc.SourceUrl, &doc.DisplayName, &doc.SourceHash,
		&doc.SourceFingerprint, &doc.SourceEtag, &doc.ContentHashVerified, &doc.MimeType,
		&doc.ResponseMimeType, &doc.DocumentKind, &doc.AcademicYear, &doc.SourceModifiedAt,
		&doc.IsCurrent, &doc.Status, &doc.PageCount, &doc.SourceSizeBytes, &doc.CharacterCount,
		&doc.WordCount, &doc.ReadingMinutes, &doc.ComplexityScore, &doc.ComplexityLabel,
		&doc.LanguageHint, &doc.ExtractorName, &doc.ExtractorVersion, &doc.IndexedAt,
		&doc.Error, &doc.DiagnosticReason, &doc.WarningsJson,
	}
}

// searchScopePG builds the admitted-document filter shared by search paths.
func searchScopePG(filter database.DocumentFilter, next int, args []any) (string, []any) {
	clause := documentAdmissionPG + ` AND d.status='ready' AND d.course_id IN` +
		pgPlaceholders(next, len(filter.CourseIDs))
	for _, id := range filter.CourseIDs {
		args = append(args, id)
	}
	next += len(filter.CourseIDs)
	if len(filter.DocumentKinds) > 0 {
		clause += ` AND d.document_kind IN` + pgPlaceholders(next, len(filter.DocumentKinds))
		for _, kind := range filter.DocumentKinds {
			args = append(args, kind)
		}
		next += len(filter.DocumentKinds)
	}
	clause += ` AND ($` + strconv.Itoa(next) + `='' OR substr(d.normalized_path,1,length($` +
		strconv.Itoa(next) + `))=$` + strconv.Itoa(next) + `)`
	return clause, append(args, filter.FolderPrefix)
}

const searchSelectPG = `d.id,d.course_id,d.course_name,d.course_short_name,d.source_path,` +
	`d.source_origin,d.normalized_path,d.source_url,d.display_name,d.source_hash,d.mime_type,` +
	`d.response_mime_type,d.document_kind,d.academic_year,d.source_modified_at,d.indexed_at,` +
	`c.id,c.ordinal,c.locator_type,c.locator_start,c.locator_end,c.heading,c.metadata_json,` +
	`substr(coalesce(c.text,''),1,400),coalesce(c.text,'')`

func scanSearchCandidate(rows nativeRows, withVector bool) (database.EmbeddedCandidate, error) {
	var out database.EmbeddedCandidate
	doc := &out.Document
	pointers := []any{
		&doc.ID, &doc.CourseID, &doc.CourseName, &doc.CourseShortName, &doc.SourcePath,
		&doc.SourceOrigin, &doc.NormalizedPath, &doc.SourceUrl, &doc.DisplayName, &doc.SourceHash,
		&doc.MimeType, &doc.ResponseMimeType, &doc.DocumentKind, &doc.AcademicYear,
		&doc.SourceModifiedAt, &doc.IndexedAt,
		&out.ChunkID, &out.Ordinal, &out.LocatorType, &out.LocatorStart, &out.LocatorEnd,
		&out.Heading, &out.MetadataJSON, &out.Excerpt, &out.Text,
	}
	if withVector {
		pointers = append(pointers, &out.Vector)
	}
	return out, rows.Scan(pointers...)
}

func (d postgresDocuments) VisibleCourseIDs(ctx context.Context) ([]int64, error) {
	rows, err := d.db.Query(ctx, `SELECT id FROM app.courses WHERE hidden=0 ORDER BY id`)
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

func (d postgresDocuments) CourseVisible(ctx context.Context, course int64) (bool, error) {
	var visible bool
	err := d.db.QueryRow(ctx, `SELECT true FROM app.courses WHERE id=$1 AND hidden=0`, course).Scan(&visible)
	if err != nil {
		return false, err
	}
	return visible, nil
}
func (d postgresDocuments) DocumentAdmitted(ctx context.Context, id string) (bool, error) {
	var admitted bool
	err := d.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge.documents d WHERE d.id=$1 AND `+
		documentAdmissionPG+`)`, id).Scan(&admitted)
	return admitted, err
}

func (d postgresDocuments) GetReadableDocument(ctx context.Context, id string) (database.KnowledgeDocument, error) {
	var doc database.KnowledgeDocument
	err := d.db.QueryRow(ctx, `SELECT `+documentColumnsPG+` FROM knowledge.documents d`+
		` JOIN app.courses c ON c.id=d.course_id AND c.hidden=0`+
		` WHERE d.id=$1 AND d.is_current=1 AND `+documentAdmissionPG, id).
		Scan(scanDocumentPointers(&doc)...)
	return doc, err
}

func (d postgresDocuments) ListMaterials(ctx context.Context, course int64, cursor, prefix, kind string,
	since *string, limit int) ([]database.KnowledgeDocument, error) {
	query := `SELECT ` + documentColumnsPG + ` FROM knowledge.documents d` +
		` WHERE ` + documentAdmissionPG + ` AND d.course_id=$1 AND ($2='' OR d.id>$2)` +
		` AND ($3='' OR substr(d.normalized_path,1,length($3))=$3) AND ($4='' OR d.document_kind=$4)` +
		` ORDER BY d.id`
	args := []any{course, cursor, prefix, kind}
	if since == nil {
		query += ` LIMIT $5`
		args = append(args, limit+1)
	}
	return listMaterialRows(ctx, d.db, query, args, since, limit)
}

func (d postgresDocuments) ListAdminDocuments(ctx context.Context, courses []int64, status, query string,
	limit int) ([]database.AdminDocument, error) {
	args := coursesArgs(courses)
	next := len(args) + 1
	args = append(args, status)
	statement := `SELECT ` + documentColumnsPG +
		`,(SELECT count(*) FROM knowledge.chunks c WHERE c.document_id=d.id)` +
		`,(SELECT count(DISTINCT c.id) FROM knowledge.chunks c` +
		` JOIN knowledge.chunk_embeddings e ON e.chunk_id=c.id WHERE c.document_id=d.id)` +
		` FROM knowledge.documents d WHERE d.course_id IN` + pgPlaceholders(1, len(courses)) +
		` AND ($` + strconv.Itoa(next) + `='' OR d.status=$` + strconv.Itoa(next) + `)` +
		` ORDER BY coalesce(d.indexed_at,'') DESC,d.source_path COLLATE "C"`
	if query == "" {
		statement += ` LIMIT $` + strconv.Itoa(next+1)
		args = append(args, limit)
	}
	return listAdminDocumentRows(ctx, d.db, statement, args, query, limit)
}

func (d postgresDocuments) LexicalCandidates(ctx context.Context, filter database.DocumentFilter,
	terms []string, limit int) ([]database.SearchCandidate, error) {
	scope, args := searchScopePG(filter, 1, nil)
	return lexicalCandidates(ctx, d.db, `SELECT `+searchSelectPG+` FROM knowledge.chunks_fts f`+
		` JOIN knowledge.chunks c ON c.id=f.chunk_id JOIN knowledge.documents d ON d.id=c.document_id`+
		` WHERE `+scope+` ORDER BY d.id,c.ordinal`, args, terms, limit)
}
