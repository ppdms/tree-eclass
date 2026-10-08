// Package queries owns the application's query surface over rdbms.DBTX.
//
// Driver selection: New(db) builds the generated postgres Queries over a
// native pgx handle (postgres path, unchanged). NewSQLite(db) builds the
// hand-written SQLiteQueries over an rdbms handle (sqlite path, same method
// set and row shapes). Callers MUST go through ForPool/ForTx below and never
// unwrap driver handles themselves.
package queries

import (
	"context"

	"tree-eclass/internal/infrastructure/rdbms"
)

// Querier is the driver-neutral query surface: every method the generated
// Queries and the hand-written SQLiteQueries both implement. Domain code
// programs against Querier and stays backend-agnostic.
type Querier interface {
	ActivityPage(ctx context.Context, arg ActivityPageParams) ([][]byte, error)
	AddCourse(ctx context.Context, arg AddCourseParams) error
	Course(ctx context.Context, id int64) (AppCourse, error)
	CourseCoverage(ctx context.Context) ([]CourseCoverageRow, error)
	HideCourse(ctx context.Context, arg HideCourseParams) (int64, error)
	ListCourses(ctx context.Context, includeHidden bool) ([]AppCourse, error)
	OrderCourse(ctx context.Context, arg OrderCourseParams) error
	RecentMaterials(ctx context.Context) ([]RecentMaterialsRow, error)
	RenameCourse(ctx context.Context, arg RenameCourseParams) (int64, error)
	SetStudyLevel(ctx context.Context, arg SetStudyLevelParams) error
	StudyLevels(ctx context.Context) ([]StudyLevelsRow, error)
	IndexEmbedding(ctx context.Context, arg IndexEmbeddingParams) error
	ObserveDocument(ctx context.Context, arg ObserveDocumentParams) error
	MaterialMetadata(ctx context.Context, arg MaterialMetadataParams) error
	ExternalMaterials(ctx context.Context, courseID int64) ([]ExternalMaterialsRow, error)
	Material(ctx context.Context, arg MaterialParams) (MaterialRow, error)
	IndexDocument(ctx context.Context, id string) (KnowledgeDocument, error)
	ReplaceChunks(ctx context.Context, documentID string) error
	InsertChunk(ctx context.Context, arg InsertChunkParams) error
	IndexChunkSearch(ctx context.Context, arg IndexChunkSearchParams) error
	MarkIndexed(ctx context.Context, arg MarkIndexedParams) error
	ExternalMirrorFiles(ctx context.Context, courseID int64) ([]ExternalMirrorFilesRow, error)
	RegisterObject(ctx context.Context, arg RegisterObjectParams) (int64, error)
	RegisterRevision(ctx context.Context, arg RegisterRevisionParams) error
	DocumentObject(ctx context.Context, arg DocumentObjectParams) (DocumentObjectRow, error)
	FileObject(ctx context.Context, id string) (FileObjectRow, error)
	QueueLock(ctx context.Context, hashtext string) error
	PendingCommand(ctx context.Context, arg PendingCommandParams) (string, error)
	EnqueueCommand(ctx context.Context, arg EnqueueCommandParams) error
	ClaimCommands(ctx context.Context, arg ClaimCommandsParams) ([]AppControlCommand, error)
	CompleteCommand(ctx context.Context, id string) error
	FailCommand(ctx context.Context, arg FailCommandParams) error
	RecoverCommands(ctx context.Context) error
	TreeNodes(ctx context.Context, courseID int64) ([]TreeNodesRow, error)
	TreeFiles(ctx context.Context, courseID int64) ([]TreeFilesRow, error)
}

// ForDBTX selects the query implementation for a bare DBTX handle (neither
// pool nor tx made explicit). Used by helpers that accept rdbms.DBTX.
func ForDBTX(db rdbms.DBTX) Querier {
	if native, ok := rdbms.UnwrapDBTX(db); ok {
		return New(native)
	}
	return NewSQLiteQueries(db)
}

// ForPool selects the query implementation for a pool handle.
func ForPool(db rdbms.Pool) Querier {
	if native, ok := rdbms.UnwrapPostgres(db); ok {
		return New(native)
	}
	return NewSQLiteQueries(db)
}

// ForTx selects the query implementation for a transaction handle.
func ForTx(tx rdbms.Tx) Querier {
	if native, ok := rdbms.UnwrapPostgresTx(tx); ok {
		return New(native)
	}
	return NewSQLiteQueries(tx)
}
