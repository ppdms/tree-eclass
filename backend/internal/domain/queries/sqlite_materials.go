package queries

import (
	"context"
)

// SQLite material methods. Plain-text SQL is reused from the generated
// consts in materials.sql.go (same package, byte-identical by
// construction); the sqlite driver rewrites $N placeholders at Exec/Query
// time. Activity feed assembly lives in sqlite_activitypage.go.

// sqliteRowScanner covers both rdbms.Row and rdbms.Rows for shared Scan
// helpers.
type sqliteRowScanner interface {
	Scan(dest ...any) error
}

func (q *SQLiteQueries) ObserveDocument(ctx context.Context, arg ObserveDocumentParams) error {
	_, err := q.db.Exec(ctx, observeDocument,
		arg.ID,
		arg.CourseID,
		arg.CourseName,
		arg.CourseShortName,
		arg.SourcePath,
		arg.NormalizedPath,
		arg.DisplayName,
		arg.SourceHash,
		arg.MimeType,
		arg.DocumentKind,
		arg.SourceSizeBytes,
	)
	return err
}

func (q *SQLiteQueries) MaterialMetadata(ctx context.Context, arg MaterialMetadataParams) error {
	_, err := q.db.Exec(ctx, materialMetadata, arg.CourseID, arg.SourcePath, arg.MaterialType)
	return err
}

func scanExternalMaterialsRow(row sqliteRowScanner) (ExternalMaterialsRow, error) {
	var i ExternalMaterialsRow
	err := row.Scan(
		&i.ID,
		&i.CourseID,
		&i.CourseName,
		&i.CourseShortName,
		&i.SourcePath,
		&i.SourceOrigin,
		&i.NormalizedPath,
		&i.SourceUrl,
		&i.DisplayName,
		&i.SourceHash,
		&i.SourceFingerprint,
		&i.SourceEtag,
		&i.ContentHashVerified,
		&i.MimeType,
		&i.ResponseMimeType,
		&i.DocumentKind,
		&i.AcademicYear,
		&i.SourceModifiedAt,
		&i.IsCurrent,
		&i.Status,
		&i.PageCount,
		&i.SourceSizeBytes,
		&i.CharacterCount,
		&i.WordCount,
		&i.ReadingMinutes,
		&i.ComplexityScore,
		&i.ComplexityLabel,
		&i.LanguageHint,
		&i.ExtractorName,
		&i.ExtractorVersion,
		&i.IndexedAt,
		&i.Error,
		&i.DiagnosticReason,
		&i.WarningsJson,
		&i.MaterialType,
		&i.SourceLabel,
	)
	return i, err
}

func (q *SQLiteQueries) ExternalMaterials(ctx context.Context, courseID int64) ([]ExternalMaterialsRow, error) {
	rows, err := q.db.Query(ctx, externalMaterials, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ExternalMaterialsRow{}
	for rows.Next() {
		i, err := scanExternalMaterialsRow(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (q *SQLiteQueries) Material(ctx context.Context, arg MaterialParams) (MaterialRow, error) {
	row := q.db.QueryRow(ctx, material, arg.CourseID, arg.ID)
	var i MaterialRow
	err := row.Scan(
		&i.ID,
		&i.CourseID,
		&i.CourseName,
		&i.CourseShortName,
		&i.SourcePath,
		&i.SourceOrigin,
		&i.NormalizedPath,
		&i.SourceUrl,
		&i.DisplayName,
		&i.SourceHash,
		&i.SourceFingerprint,
		&i.SourceEtag,
		&i.ContentHashVerified,
		&i.MimeType,
		&i.ResponseMimeType,
		&i.DocumentKind,
		&i.AcademicYear,
		&i.SourceModifiedAt,
		&i.IsCurrent,
		&i.Status,
		&i.PageCount,
		&i.SourceSizeBytes,
		&i.CharacterCount,
		&i.WordCount,
		&i.ReadingMinutes,
		&i.ComplexityScore,
		&i.ComplexityLabel,
		&i.LanguageHint,
		&i.ExtractorName,
		&i.ExtractorVersion,
		&i.IndexedAt,
		&i.Error,
		&i.DiagnosticReason,
		&i.WarningsJson,
		&i.MaterialType,
		&i.SourceLabel,
	)
	return i, err
}

func (q *SQLiteQueries) IndexDocument(ctx context.Context, id string) (KnowledgeDocument, error) {
	row := q.db.QueryRow(ctx, indexDocument, id)
	var i KnowledgeDocument
	err := row.Scan(
		&i.ID,
		&i.CourseID,
		&i.CourseName,
		&i.CourseShortName,
		&i.SourcePath,
		&i.SourceOrigin,
		&i.NormalizedPath,
		&i.SourceUrl,
		&i.DisplayName,
		&i.SourceHash,
		&i.SourceFingerprint,
		&i.SourceEtag,
		&i.ContentHashVerified,
		&i.MimeType,
		&i.ResponseMimeType,
		&i.DocumentKind,
		&i.AcademicYear,
		&i.SourceModifiedAt,
		&i.IsCurrent,
		&i.Status,
		&i.PageCount,
		&i.SourceSizeBytes,
		&i.CharacterCount,
		&i.WordCount,
		&i.ReadingMinutes,
		&i.ComplexityScore,
		&i.ComplexityLabel,
		&i.LanguageHint,
		&i.ExtractorName,
		&i.ExtractorVersion,
		&i.IndexedAt,
		&i.Error,
		&i.DiagnosticReason,
		&i.WarningsJson,
	)
	return i, err
}

func (q *SQLiteQueries) ReplaceChunks(ctx context.Context, documentID string) error {
	_, err := q.db.Exec(ctx, replaceChunks, documentID)
	return err
}

func (q *SQLiteQueries) InsertChunk(ctx context.Context, arg InsertChunkParams) error {
	_, err := q.db.Exec(ctx, insertChunk,
		arg.ID,
		arg.DocumentID,
		arg.Ordinal,
		arg.LocatorType,
		arg.LocatorStart,
		arg.LocatorEnd,
		arg.Heading,
		arg.Text,
		arg.NormalizedText,
		arg.ContentHash,
		arg.MetadataJson,
	)
	return err
}

func (q *SQLiteQueries) IndexChunkSearch(ctx context.Context, arg IndexChunkSearchParams) error {
	_, err := q.db.Exec(ctx, indexChunkSearch,
		arg.ChunkID,
		arg.Text,
		arg.NormalizedText,
		arg.Heading,
		arg.DisplayName,
		arg.SourcePath,
		arg.CourseName,
	)
	return err
}

func (q *SQLiteQueries) MarkIndexed(ctx context.Context, arg MarkIndexedParams) error {
	_, err := q.db.Exec(ctx, markIndexed,
		arg.ID,
		arg.PageCount,
		arg.CharacterCount,
		arg.WordCount,
		arg.ReadingMinutes,
		arg.IndexedAt,
		arg.WarningsJson,
		arg.ComplexityScore,
		arg.ComplexityLabel,
	)
	return err
}
