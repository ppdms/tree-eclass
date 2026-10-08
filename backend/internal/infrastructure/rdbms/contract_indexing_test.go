package rdbms_test

import (
	"context"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func TestIndexingEclassUpdateAndArchiveMembership(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			ctx := t.Context()
			if err := (courses.Service{Pool: store}).Add(ctx, 91, "contract"); err != nil {
				t.Fatal(err)
			}
			doc, encoded := seedIndexingEclass(t, store)
			second := assertIndexingUpdate(t, store, doc, encoded)
			member := assertArchivePublish(t, store, doc, second)
			assertArchiveRetire(t, store, doc, member)
			assertArchiveRollback(t, store)
		})
	}
}

func seedIndexingEclass(t *testing.T, store database.Store) (string, string) {
	t.Helper()
	ctx := t.Context()
	path := "/eclass/notes.pdf"
	doc, encoded, first := identity.Document(91, path), identity.Encode(path), identity.TextHash("indexing-v1")
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := seedIndexingRevision(ctx, tx, "rev_indexing_v1", doc, 91, encoded, first); err != nil {
		t.Fatal(err)
	}
	status, err := tx.Indexing().ObserveEclassDocument(ctx, indexingEclassParams(doc, encoded, first))
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("fresh eClass status = %q; want pending", status)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read, err := store.Indexing().IndexDocument(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if read.SourceHash != first || read.SourceOrigin != "eclass" || read.IsCurrent != 1 {
		t.Fatalf("fresh eClass row = %+v", read)
	}
	return doc, encoded
}

func assertIndexingUpdate(t *testing.T, store database.Store, doc, encoded string) string {
	t.Helper()
	ctx := t.Context()
	second := identity.TextHash("indexing-v2")
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := seedIndexingRevision(ctx, tx, "rev_indexing_v2", doc, 91, encoded, second); err != nil {
		t.Fatal(err)
	}
	status, err := tx.Indexing().ObserveEclassDocument(ctx, indexingEclassParams(doc, encoded, second))
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("updated eClass status = %q; want pending", status)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return second
}

func assertArchivePublish(t *testing.T, store database.Store, doc, parentHash string) string {
	t.Helper()
	ctx := t.Context()
	member, memberPath, memberHash := "doc_archive_member", identity.Encode("/archive/member.txt"),
		identity.TextHash("archive-member")
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := seedIndexingRevision(ctx, tx, "rev_archive_member", member, 91, memberPath, memberHash); err != nil {
		t.Fatal(err)
	}
	status, err := tx.Indexing().UpsertArchiveDocument(ctx, database.ArchiveDocumentParams{
		ID: member, CourseID: 91, CourseName: "contract", Path: memberPath,
		Name: identity.Encode("member.txt"), SHA: memberHash,
		Media: "text/plain", Kind: "text", Bytes: 7, SourceOrigin: "eclass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("archive member status = %q; want pending", status)
	}
	if err := tx.Indexing().UpsertArchiveMember(ctx, indexingMemberParams(doc, member, memberPath, memberHash,
		parentHash)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	memberRow, err := store.Indexing().IndexDocument(ctx, member)
	if err != nil {
		t.Fatal(err)
	}
	if memberRow.SourceHash != memberHash || memberRow.IsCurrent != 1 || memberRow.Status != "pending" {
		t.Fatalf("archive member row = %+v", memberRow)
	}
	updated, err := store.Indexing().IndexDocument(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if updated.SourceHash != parentHash {
		t.Fatalf("updated eClass hash = %q; want %q", updated.SourceHash, parentHash)
	}
	return member
}

func assertArchiveRetire(t *testing.T, store database.Store, doc, member string) {
	t.Helper()
	ctx := t.Context()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Indexing().RetireMissingArchiveMembers(ctx, doc, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Indexing().IndexDocument(ctx, member); !database.IsNoRows(err) {
		t.Fatalf("retired member still current: %v", err)
	}
}

func assertArchiveRollback(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	rolledBack := "doc_archive_rolled_back"
	if err := seedIndexingRevision(ctx, tx, "rev_archive_rolled_back", rolledBack, 91,
		identity.Encode("/archive/rolled.txt"), identity.TextHash("rolled")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Indexing().UpsertArchiveDocument(ctx, database.ArchiveDocumentParams{
		ID: rolledBack, CourseID: 91, CourseName: "contract",
		Path: identity.Encode("/archive/rolled.txt"), Name: identity.Encode("rolled.txt"),
		SHA: identity.TextHash("rolled"), Media: "text/plain", Kind: "text",
		Bytes: 6, SourceOrigin: "eclass",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Indexing().IndexDocument(ctx, rolledBack); !database.IsNoRows(err) {
		t.Fatalf("rolled-back member visible: %v", err)
	}
}

func indexingMemberParams(doc, member, memberPath, memberHash, parentHash string) database.ArchiveMemberParams {
	return database.ArchiveMemberParams{
		ChildDocumentID: member, ParentDocumentID: doc, Path: memberPath, ChainJSON: "[]",
		Depth: 1, ArchiveFormat: "zip", ParentHash: parentHash, ParentPrint: "",
		MemberHash: memberHash, CRC32: 42, CompressedSize: 7, ExpandedSize: 7,
		MemberKind: "text", Media: "text/plain",
	}
}

func indexingEclassParams(doc, encoded, hash string) database.EclassDocumentParams {
	return database.EclassDocumentParams{
		Document: doc, CourseID: 91, CourseName: "contract", Path: encoded,
		Name: identity.Encode("notes.pdf"), SHA: hash, Kind: "text", Status: "pending",
	}
}

func seedIndexingRevision(ctx context.Context, tx database.Tx, rev, doc string, course int64, path, hash string) error {
	if err := tx.Objects().RegisterObject(ctx, database.ObjectReference{
		Bucket: "contract", Key: hash, VersionID: hash, SHA256: hash, Bytes: 8, MediaType: "text/plain",
	}); err != nil {
		return err
	}
	return tx.Objects().RegisterRevision(ctx, database.RegisterRevisionParams{
		ID: rev, DocumentID: doc, CourseID: course, LogicalPath: path, ObjectID: hash,
	})
}
