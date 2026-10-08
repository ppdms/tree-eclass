package rdbms

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
)

// sqliteIndexing implements database.Indexing with native SQLite SQL: ?
// placeholders, unqualified table names, no row locks, and strftime clocks.
// Writer transactions serialize every mutation; pure SELECT locks are
// validity probes only.
type sqliteIndexing struct{ db nativeDBTX }

const sqliteIndexingDocumentColumns = `id,course_id,course_name,course_short_name,source_path,` +
	`source_origin,normalized_path,source_url,display_name,source_hash,source_fingerprint,` +
	`source_etag,content_hash_verified,mime_type,response_mime_type,document_kind,` +
	`academic_year,source_modified_at,is_current,status,page_count,source_size_bytes,` +
	`character_count,word_count,reading_minutes,complexity_score,complexity_label,` +
	`language_hint,extractor_name,extractor_version,indexed_at,error,` +
	`diagnostic_reason,warnings_json`

func scanSQLiteIndexingDocument(row nativeRow) (database.KnowledgeDocument, error) {
	var doc database.KnowledgeDocument
	err := row.Scan(
		&doc.ID,
		&doc.CourseID,
		&doc.CourseName,
		&doc.CourseShortName,
		&doc.SourcePath,
		&doc.SourceOrigin,
		&doc.NormalizedPath,
		&doc.SourceUrl,
		&doc.DisplayName,
		&doc.SourceHash,
		&doc.SourceFingerprint,
		&doc.SourceEtag,
		&doc.ContentHashVerified,
		&doc.MimeType,
		&doc.ResponseMimeType,
		&doc.DocumentKind,
		&doc.AcademicYear,
		&doc.SourceModifiedAt,
		&doc.IsCurrent,
		&doc.Status,
		&doc.PageCount,
		&doc.SourceSizeBytes,
		&doc.CharacterCount,
		&doc.WordCount,
		&doc.ReadingMinutes,
		&doc.ComplexityScore,
		&doc.ComplexityLabel,
		&doc.LanguageHint,
		&doc.ExtractorName,
		&doc.ExtractorVersion,
		&doc.IndexedAt,
		&doc.Error,
		&doc.DiagnosticReason,
		&doc.WarningsJson,
	)
	return doc, err
}

func (x sqliteIndexing) IndexDocument(ctx context.Context, id string) (database.KnowledgeDocument, error) {
	return scanSQLiteIndexingDocument(x.db.QueryRow(ctx,
		`SELECT `+sqliteIndexingDocumentColumns+` FROM documents WHERE id=? AND is_current=1`, id))
}

func (x sqliteIndexing) ObserveDocument(ctx context.Context, params database.ObserveDocumentParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,mime_type,document_kind,source_size_bytes,status,source_origin,content_hash_verified)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?,'pending','external',1)`,
		params.ID,
		params.CourseID,
		params.CourseName,
		params.CourseShortName,
		params.SourcePath,
		params.NormalizedPath,
		params.DisplayName,
		params.SourceHash,
		params.MimeType,
		params.DocumentKind,
		params.SourceSizeBytes,
	)
	return err
}

func (x sqliteIndexing) ObserveEclassDocument(
	ctx context.Context,
	params database.EclassDocumentParams,
) (string, error) {
	var status string
	// SQLite UPSERT cannot carry a WHERE filter on the target row; the
	// eclass-origin guard runs as an explicit pre-check so an external row
	// with the same id never gains eclass content. The writer transaction
	// serializes the check with the upsert.
	var origin *string
	err := x.db.QueryRow(ctx, `SELECT source_origin FROM documents WHERE id=?`, params.Document).Scan(&origin)
	if err != nil && !database.IsNoRows(err) {
		return "", err
	}
	if origin != nil && *origin != "eclass" {
		return "", database.ErrNoRows
	}
	err = x.db.QueryRow(ctx, `INSERT INTO documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,source_url,source_etag,mime_type,document_kind,source_size_bytes,`+
		`status,source_origin,content_hash_verified,source_modified_at)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'eclass',1,?)`+
		` ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,`+
		`course_short_name=excluded.course_short_name,display_name=excluded.display_name,`+
		`source_hash=excluded.source_hash,source_url=excluded.source_url,`+
		`source_etag=excluded.source_etag,mime_type=excluded.mime_type,`+
		`document_kind=excluded.document_kind,source_size_bytes=excluded.source_size_bytes,`+
		`source_modified_at=excluded.source_modified_at,is_current=1,content_hash_verified=1,`+
		`status=CASE WHEN documents.source_hash<>excluded.source_hash `+
		`OR documents.is_current=0 THEN excluded.status `+
		`ELSE documents.status END`+
		` RETURNING status`,
		params.Document,
		params.CourseID,
		params.CourseName,
		params.CourseShortName,
		params.Path,
		params.Path,
		params.Name,
		params.SHA,
		params.URL,
		params.ETag,
		params.Media,
		params.Kind,
		params.Bytes,
		params.Status,
		params.Updated,
	).Scan(&status)
	return status, err
}

func (x sqliteIndexing) StartIndexRun(ctx context.Context, params database.StartIndexRunParams) error {
	result, err := x.db.Exec(ctx, `UPDATE documents`+
		` SET status='running',error=NULL,diagnostic_reason=NULL`+
		` WHERE id=? AND source_hash=? AND is_current=1`, params.ID, params.Hash)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return database.ErrNoRows
	}
	return nil
}

func (x sqliteIndexing) RecordIndexFailure(ctx context.Context, params database.RecordIndexFailureParams) error {
	_, err := x.db.Exec(ctx, `UPDATE documents`+
		` SET status=?,error=?,diagnostic_reason=?`+
		` WHERE id=? AND source_hash=? AND is_current=1 AND status='running'`,
		params.Status, params.Error, params.Reason, params.ID, params.Hash)
	return err
}

func (x sqliteIndexing) LockCourseForIndex(ctx context.Context, courseID int64) error {
	// The admitted writer owns the course row until commit; the probe
	// keeps the missing-course signal without row locks.
	var id int64
	return x.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`, courseID).Scan(&id)
}

func (x sqliteIndexing) LockDocumentForIndex(ctx context.Context, id string) error {
	var found string
	return x.db.QueryRow(ctx, `SELECT id FROM documents WHERE id=?`, id).Scan(&found)
}

func (x sqliteIndexing) ReplaceChunks(ctx context.Context, documentID string) error {
	_, err := x.db.Exec(ctx, `DELETE FROM chunks WHERE document_id=?`, documentID)
	return err
}

func (x sqliteIndexing) InsertChunk(ctx context.Context, params database.InsertChunkParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO chunks(`+
		`id,document_id,ordinal,locator_type,locator_start,locator_end,heading,text,`+
		`normalized_text,content_hash,metadata_json)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		params.ID, params.DocumentID, params.Ordinal, params.LocatorType,
		params.LocatorStart, params.LocatorEnd, params.Heading,
		params.Text, params.NormalizedText, params.ContentHash, params.MetadataJSON)
	return err
}

func (x sqliteIndexing) IndexChunkSearch(ctx context.Context, params database.IndexChunkSearchParams) error {
	// AFTER INSERT/UPDATE triggers maintain search_vector as the lowered
	// concatenation of the same text columns.
	_, err := x.db.Exec(ctx, `INSERT INTO chunks_fts(`+
		`chunk_id,text,normalized_text,heading,display_name,source_path,course_name)`+
		` VALUES(?,?,?,?,?,?,?)`,
		params.ChunkID, params.Text, params.NormalizedText, params.Heading,
		params.DisplayName, params.SourcePath, params.CourseName)
	return err
}

func (x sqliteIndexing) IndexEmbedding(ctx context.Context, params database.IndexEmbeddingParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO chunk_embeddings(chunk_id,model,vector,dimensions)`+
		` VALUES(?,?,?,?)`+
		` ON CONFLICT(chunk_id,model)`+
		` DO UPDATE SET vector=excluded.vector,dimensions=excluded.dimensions`,
		params.ChunkID, params.Model, params.Vector, params.Dimensions)
	return err
}

func (x sqliteIndexing) MarkIndexed(ctx context.Context, params database.MarkIndexedParams) error {
	_, err := x.db.Exec(ctx, `UPDATE documents`+
		` SET status='ready',page_count=?,character_count=?,word_count=?,reading_minutes=?,`+
		`extractor_name='tree-parser',extractor_version='1',indexed_at=?,error=NULL,warnings_json=?,`+
		`complexity_score=?,complexity_label=? WHERE id=?`,
		params.PageCount, params.CharacterCount, params.WordCount,
		params.ReadingMinutes, params.IndexedAt, params.WarningsJSON,
		params.ComplexityScore, params.ComplexityLabel, params.ID)
	return err
}

func (x sqliteIndexing) UpsertArchiveDocument(
	ctx context.Context,
	params database.ArchiveDocumentParams,
) (string, error) {
	// SQLite UPSERT cannot carry a WHERE filter on the target row; the
	// course/path/origin guard runs as an explicit pre-check so a foreign
	// row with the same id never gains member content. The writer
	// transaction serializes the check with the upsert.
	var guard struct {
		course int64
		path   string
		origin string
	}
	err := x.db.QueryRow(ctx, `SELECT course_id,normalized_path,source_origin FROM documents WHERE id=?`,
		params.ID).Scan(&guard.course, &guard.path, &guard.origin)
	if err != nil && !database.IsNoRows(err) {
		return "", err
	}
	if err == nil && (guard.course != params.CourseID || guard.path != params.Path ||
		guard.origin != params.SourceOrigin) {
		return "", database.ErrNoRows
	}
	var status string
	err = x.db.QueryRow(ctx, `INSERT INTO documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,source_fingerprint,mime_type,document_kind,source_size_bytes,status,`+
		`source_origin,content_hash_verified)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,1)`+
		` ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,`+
		`course_short_name=excluded.course_short_name,display_name=excluded.display_name,`+
		`source_hash=excluded.source_hash,source_fingerprint=excluded.source_fingerprint,`+
		`mime_type=excluded.mime_type,document_kind=excluded.document_kind,`+
		`source_size_bytes=excluded.source_size_bytes,is_current=1,content_hash_verified=1,`+
		`status=CASE WHEN documents.source_hash=excluded.source_hash`+
		` AND documents.is_current=1 AND documents.status='ready'`+
		` THEN 'ready' ELSE 'pending' END`+
		` RETURNING status`,
		params.ID, params.CourseID, params.CourseName, params.CourseShortName,
		params.Path, params.Path, params.Name, params.SHA, params.SHA, params.Media, params.Kind,
		params.Bytes, params.SourceOrigin,
	).Scan(&status)
	if err != nil {
		return "", err
	}
	_, err = x.db.Exec(ctx, `UPDATE document_revisions SET deleted_at=NULL`+
		` WHERE document_id=? AND object_id=? AND course_id=? AND logical_path=?`,
		params.ID, params.SHA, params.CourseID, params.Path)
	if err != nil {
		return "", err
	}
	return status, nil
}

func (x sqliteIndexing) UpsertArchiveMember(ctx context.Context, params database.ArchiveMemberParams) error {
	// The same-parent guard runs as an explicit pre-check: SQLite UPSERT
	// cannot filter the target row with WHERE.
	var parent *string
	err := x.db.QueryRow(ctx, `SELECT parent_document_id FROM archive_members WHERE child_document_id=?`,
		params.ChildDocumentID).Scan(&parent)
	if err != nil && !database.IsNoRows(err) {
		return err
	}
	if parent != nil && *parent != params.ParentDocumentID {
		return nil
	}
	_, err = x.db.Exec(ctx, `INSERT INTO archive_members(`+
		`child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,`+
		`depth,archive_format,parent_source_hash,parent_source_fingerprint,member_hash,crc32,`+
		`compressed_size,expanded_size,member_kind,mime_type)`+
		` VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`+
		` ON CONFLICT(child_document_id) DO UPDATE SET member_path=excluded.member_path,`+
		`normalized_member_path=excluded.normalized_member_path,member_chain_json=excluded.member_chain_json,`+
		`depth=excluded.depth,archive_format=excluded.archive_format,`+
		`parent_source_hash=excluded.parent_source_hash,`+
		`parent_source_fingerprint=excluded.parent_source_fingerprint,`+
		`member_hash=excluded.member_hash,crc32=excluded.crc32,compressed_size=excluded.compressed_size,`+
		`expanded_size=excluded.expanded_size,member_kind=excluded.member_kind,mime_type=excluded.mime_type`,
		params.ChildDocumentID, params.ParentDocumentID, params.Path, params.Path, params.ChainJSON,
		params.Depth, params.ArchiveFormat, params.ParentHash, params.ParentPrint,
		params.MemberHash, params.CRC32, params.CompressedSize, params.ExpandedSize,
		params.MemberKind, params.Media)
	return err
}

func (x sqliteIndexing) RetireMissingArchiveMembers(
	ctx context.Context,
	parent string,
	current []string,
) error {
	args := []any{parent}
	filter := ""
	if len(current) > 0 {
		marks := make([]string, 0, len(current))
		for _, id := range current {
			marks = append(marks, "?")
			args = append(args, id)
		}
		filter = " AND documents.id NOT IN (" + strings.Join(marks, ",") + ")"
	}
	_, err := x.db.Exec(ctx, `UPDATE documents SET is_current=0`+
		` WHERE is_current=1 AND id IN(`+
		`SELECT child_document_id FROM archive_members WHERE parent_document_id=?)`+filter,
		args...)
	return err
}

func (x sqliteIndexing) RequeueDocument(ctx context.Context, params database.RequeueDocumentParams) error {
	_, err := x.db.Exec(ctx, `UPDATE documents`+
		` SET source_hash=?,source_size_bytes=?,status='pending' WHERE id=?`,
		params.Hash, params.Bytes, params.ID)
	return err
}
