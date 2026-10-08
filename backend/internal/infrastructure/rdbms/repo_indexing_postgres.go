package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

// postgresIndexing implements database.Indexing with native PostgreSQL SQL:
// $N placeholders, schema-qualified tables, row locks, gen_random_uuid and
// now() clocks.
type postgresIndexing struct{ db nativeDBTX }

const indexingDocumentColumns = `id,course_id,course_name,course_short_name,source_path,` +
	`source_origin,normalized_path,source_url,display_name,source_hash,source_fingerprint,` +
	`source_etag,content_hash_verified,mime_type,response_mime_type,document_kind,` +
	`academic_year,source_modified_at,is_current,status,page_count,source_size_bytes,` +
	`character_count,word_count,reading_minutes,complexity_score,complexity_label,` +
	`language_hint,extractor_name,extractor_version,indexed_at,error,` +
	`diagnostic_reason,warnings_json`

func scanIndexingDocument(row nativeRow) (database.KnowledgeDocument, error) {
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

func (x postgresIndexing) IndexDocument(ctx context.Context, id string) (database.KnowledgeDocument, error) {
	return scanIndexingDocument(x.db.QueryRow(ctx,
		`SELECT `+indexingDocumentColumns+` FROM knowledge.documents WHERE id=$1 AND is_current=1`, id))
}

func (x postgresIndexing) ObserveDocument(ctx context.Context, params database.ObserveDocumentParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO knowledge.documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,mime_type,document_kind,source_size_bytes,status,source_origin,content_hash_verified)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending','external',1)`,
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

func (x postgresIndexing) ObserveEclassDocument(
	ctx context.Context,
	params database.EclassDocumentParams,
) (string, error) {
	var status string
	err := x.db.QueryRow(ctx, `INSERT INTO knowledge.documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,source_url,source_etag,mime_type,document_kind,source_size_bytes,`+
		`status,source_origin,content_hash_verified,source_modified_at)`+
		` VALUES($1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$10,$11,$12,$13,'eclass',1,$14)`+
		` ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,`+
		`course_short_name=excluded.course_short_name,display_name=excluded.display_name,`+
		`source_hash=excluded.source_hash,source_url=excluded.source_url,`+
		`source_etag=excluded.source_etag,mime_type=excluded.mime_type,`+
		`document_kind=excluded.document_kind,source_size_bytes=excluded.source_size_bytes,`+
		`source_modified_at=excluded.source_modified_at,is_current=1,content_hash_verified=1,`+
		`status=CASE WHEN knowledge.documents.source_hash<>excluded.source_hash `+
		`OR knowledge.documents.is_current=0 THEN excluded.status `+
		`ELSE knowledge.documents.status END`+
		` WHERE knowledge.documents.source_origin='eclass' RETURNING status`,
		params.Document,
		params.CourseID,
		params.CourseName,
		params.CourseShortName,
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

func (x postgresIndexing) StartIndexRun(ctx context.Context, params database.StartIndexRunParams) error {
	result, err := x.db.Exec(ctx, `UPDATE knowledge.documents`+
		` SET status='running',error=NULL,diagnostic_reason=NULL`+
		` WHERE id=$1 AND source_hash=$2 AND is_current=1`, params.ID, params.Hash)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return database.ErrNoRows
	}
	return nil
}

func (x postgresIndexing) RecordIndexFailure(ctx context.Context, params database.RecordIndexFailureParams) error {
	_, err := x.db.Exec(ctx, `UPDATE knowledge.documents`+
		` SET status=$3,error=$4,diagnostic_reason=$5`+
		` WHERE id=$1 AND source_hash=$2 AND is_current=1 AND status='running'`,
		params.ID, params.Hash, params.Status, params.Error, params.Reason)
	return err
}

func (x postgresIndexing) LockCourseForIndex(ctx context.Context, courseID int64) error {
	_, err := x.db.Exec(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, courseID)
	return err
}

func (x postgresIndexing) LockDocumentForIndex(ctx context.Context, id string) error {
	_, err := x.db.Exec(ctx, `SELECT id FROM knowledge.documents WHERE id=$1 FOR UPDATE`, id)
	return err
}

func (x postgresIndexing) ReplaceChunks(ctx context.Context, documentID string) error {
	_, err := x.db.Exec(ctx, `DELETE FROM knowledge.chunks WHERE document_id=$1`, documentID)
	return err
}

func (x postgresIndexing) InsertChunk(ctx context.Context, params database.InsertChunkParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO knowledge.chunks(`+
		`id,document_id,ordinal,locator_type,locator_start,locator_end,heading,text,`+
		`normalized_text,content_hash,metadata_json)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		params.ID, params.DocumentID, params.Ordinal, params.LocatorType,
		params.LocatorStart, params.LocatorEnd, params.Heading,
		params.Text, params.NormalizedText, params.ContentHash, params.MetadataJSON)
	return err
}

func (x postgresIndexing) IndexChunkSearch(ctx context.Context, params database.IndexChunkSearchParams) error {
	// The index_chunk trigger maintains search_vector from the text columns.
	_, err := x.db.Exec(ctx, `INSERT INTO knowledge.chunks_fts(`+
		`chunk_id,text,normalized_text,heading,display_name,source_path,course_name)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7)`,
		params.ChunkID, params.Text, params.NormalizedText, params.Heading,
		params.DisplayName, params.SourcePath, params.CourseName)
	return err
}

func (x postgresIndexing) IndexEmbedding(ctx context.Context, params database.IndexEmbeddingParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO knowledge.chunk_embeddings(chunk_id,model,vector,dimensions)`+
		` VALUES($1,$2,$3,$4)`+
		` ON CONFLICT(chunk_id,model)`+
		` DO UPDATE SET vector=excluded.vector,dimensions=excluded.dimensions`,
		params.ChunkID, params.Model, params.Vector, params.Dimensions)
	return err
}

func (x postgresIndexing) MarkIndexed(ctx context.Context, params database.MarkIndexedParams) error {
	_, err := x.db.Exec(ctx, `UPDATE knowledge.documents`+
		` SET status='ready',page_count=$2,character_count=$3,word_count=$4,reading_minutes=$5,`+
		`extractor_name='tree-parser',extractor_version='1',indexed_at=$6,error=NULL,warnings_json=$7,`+
		`complexity_score=$8,complexity_label=$9 WHERE id=$1`,
		params.ID, params.PageCount, params.CharacterCount, params.WordCount,
		params.ReadingMinutes, params.IndexedAt, params.WarningsJSON,
		params.ComplexityScore, params.ComplexityLabel)
	return err
}

func (x postgresIndexing) UpsertArchiveDocument(
	ctx context.Context,
	params database.ArchiveDocumentParams,
) (string, error) {
	var status string
	err := x.db.QueryRow(ctx, `INSERT INTO knowledge.documents(`+
		`id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,`+
		`source_hash,source_fingerprint,mime_type,document_kind,source_size_bytes,status,`+
		`source_origin,content_hash_verified)`+
		` VALUES($1,$2,$3,$4,$5,$5,$6,$7,$7,$8,$9,$10,'pending',$11,1)`+
		` ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,`+
		`course_short_name=excluded.course_short_name,display_name=excluded.display_name,`+
		`source_hash=excluded.source_hash,source_fingerprint=excluded.source_fingerprint,`+
		`mime_type=excluded.mime_type,document_kind=excluded.document_kind,`+
		`source_size_bytes=excluded.source_size_bytes,is_current=1,content_hash_verified=1,`+
		`status=CASE WHEN knowledge.documents.source_hash=excluded.source_hash`+
		` AND knowledge.documents.is_current=1 AND knowledge.documents.status='ready'`+
		` THEN 'ready' ELSE 'pending' END`+
		` WHERE knowledge.documents.course_id=excluded.course_id`+
		` AND knowledge.documents.normalized_path=excluded.normalized_path`+
		` AND knowledge.documents.source_origin=excluded.source_origin RETURNING status`,
		params.ID, params.CourseID, params.CourseName, params.CourseShortName,
		params.Path, params.Name, params.SHA, params.Media, params.Kind,
		params.Bytes, params.SourceOrigin,
	).Scan(&status)
	if err != nil {
		return "", err
	}
	// A newly admitted container revision explicitly restores this member version.
	_, err = x.db.Exec(ctx, `UPDATE app.document_revisions SET deleted_at=NULL`+
		` WHERE document_id=$1 AND object_id=$2 AND course_id=$3 AND logical_path=$4`,
		params.ID, params.SHA, params.CourseID, params.Path)
	if err != nil {
		return "", err
	}
	return status, nil
}

func (x postgresIndexing) UpsertArchiveMember(ctx context.Context, params database.ArchiveMemberParams) error {
	_, err := x.db.Exec(ctx, `INSERT INTO knowledge.archive_members(`+
		`child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,`+
		`depth,archive_format,parent_source_hash,parent_source_fingerprint,member_hash,crc32,`+
		`compressed_size,expanded_size,member_kind,mime_type)`+
		` VALUES($1,$2,$3,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`+
		` ON CONFLICT(child_document_id) DO UPDATE SET member_path=excluded.member_path,`+
		`normalized_member_path=excluded.normalized_member_path,member_chain_json=excluded.member_chain_json,`+
		`depth=excluded.depth,archive_format=excluded.archive_format,`+
		`parent_source_hash=excluded.parent_source_hash,`+
		`parent_source_fingerprint=excluded.parent_source_fingerprint,`+
		`member_hash=excluded.member_hash,crc32=excluded.crc32,compressed_size=excluded.compressed_size,`+
		`expanded_size=excluded.expanded_size,member_kind=excluded.member_kind,mime_type=excluded.mime_type`+
		` WHERE knowledge.archive_members.parent_document_id=excluded.parent_document_id`,
		params.ChildDocumentID, params.ParentDocumentID, params.Path, params.ChainJSON,
		params.Depth, params.ArchiveFormat, params.ParentHash, params.ParentPrint,
		params.MemberHash, params.CRC32, params.CompressedSize, params.ExpandedSize,
		params.MemberKind, params.Media)
	return err
}

func (x postgresIndexing) RetireMissingArchiveMembers(
	ctx context.Context,
	parent string,
	current []string,
) error {
	_, err := x.db.Exec(ctx, `UPDATE knowledge.documents d SET is_current=0`+
		` WHERE d.is_current=1 AND d.id IN(`+
		`SELECT child_document_id FROM knowledge.archive_members WHERE parent_document_id=$1)`+
		` AND NOT(d.id=ANY(coalesce($2::text[],ARRAY[]::text[])))`, parent, current)
	return err
}

func (x postgresIndexing) RequeueDocument(ctx context.Context, params database.RequeueDocumentParams) error {
	_, err := x.db.Exec(ctx, `UPDATE knowledge.documents`+
		` SET source_hash=$2,source_size_bytes=$3,status='pending' WHERE id=$1`,
		params.ID, params.Hash, params.Bytes)
	return err
}

func (x postgresIndexing) LockCoursesForMaintenance(ctx context.Context) error {
	_, err := x.db.Exec(ctx, `SELECT id FROM app.courses ORDER BY id FOR UPDATE`)
	return err
}

// indexingMaintenanceCandidates selects documents whose derived index needs a
// rebuild: pending/running/failed rows, unverified content, missing chunks,
// failed index commands, or derived rows missing a search or embedding entry.
const indexingMaintenanceCandidatesPG = `SELECT d.id FROM knowledge.documents d ` +
	`JOIN app.courses c ON c.id=d.course_id` +
	` WHERE d.is_current=1 AND d.status NOT IN('unsupported','external','skipped_limit')` +
	` AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id` +
	` WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path` +
	` AND r.deleted_at IS NULL AND o.sha256=d.source_hash)` +
	` AND ($1='rebuild' OR d.status IN('pending','running','failed') OR d.content_hash_verified<>1` +
	` OR (coalesce(d.word_count,0)>0 AND NOT EXISTS(SELECT 1 FROM knowledge.chunks ch WHERE ch.document_id=d.id))` +
	` OR EXISTS(SELECT 1 FROM app.control_commands q` +
	` WHERE q.queue='index' AND q.action='index_document' AND q.status='failed'` +
	` AND q.payload->>'document_id'=d.id)` +
	` OR EXISTS(SELECT 1 FROM knowledge.chunks ch WHERE ch.document_id=d.id AND` +
	` (NOT EXISTS(SELECT 1 FROM knowledge.chunks_fts f WHERE f.chunk_id=ch.id) OR` +
	` NOT EXISTS(SELECT 1 FROM knowledge.chunk_embeddings e WHERE e.chunk_id=ch.id))))` +
	` AND ($1<>'retry_failed' OR d.status='failed' OR EXISTS(SELECT 1 FROM app.control_commands q` +
	` WHERE q.queue='index' AND q.action='index_document' AND q.status='failed'` +
	` AND q.payload->>'document_id'=d.id))`

func (x postgresIndexing) MaintainIndex(ctx context.Context, action string) error {
	switch action {
	case "reconcile", "rebuild", "retry_failed":
	default:
		return errors.New("unknown knowledge maintenance action")
	}
	_, err := x.db.Exec(ctx, `WITH candidates AS (`+indexingMaintenanceCandidatesPG+`), retried AS (`+
		` UPDATE app.control_commands q SET status='pending',attempts=0,error=NULL,claimed_at=NULL,available_at=now()`+
		` WHERE q.queue='index' AND q.action='index_document' AND q.status='failed'`+
		` AND q.payload->>'document_id' IN(SELECT id FROM candidates)`+
		`), admitted AS (`+
		` INSERT INTO app.control_commands(id,queue,action,payload)`+
		` SELECT gen_random_uuid()::text,'index','index_document',jsonb_build_object('document_id',d.id)`+
		` FROM candidates d WHERE NOT EXISTS(SELECT 1 FROM app.control_commands q`+
		` WHERE q.queue='index' AND q.action='index_document' AND q.payload->>'document_id'=d.id`+
		` AND q.status IN('pending','running','failed'))`+
		`) UPDATE knowledge.documents SET status='pending',error=NULL,diagnostic_reason=NULL`+
		` WHERE id IN(SELECT id FROM candidates)`, action)
	return err
}

func (x postgresIndexing) RetryFailedAnalyses(ctx context.Context) error {
	// Finite internal allowlist; identifiers never come from the caller.
	for _, table := range []string{
		"knowledge.document_enrichments",
		"knowledge.page_enrichments",
		"knowledge.course_blueprints",
		"knowledge.practice_question_sets",
	} {
		if _, err := x.db.Exec(ctx, `UPDATE `+table+` SET status='pending',attempts=0,error=NULL,`+
			`claimed_at=NULL,available_at=to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')`+
			` WHERE status='failed'`); err != nil {
			return err
		}
	}
	return nil
}
