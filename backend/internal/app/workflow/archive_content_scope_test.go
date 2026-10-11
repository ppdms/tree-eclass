package workflow

import (
	"testing"
)

func archiveContentScopeChecks(t *testing.T, pool *fixtureStore, base, document string) {
	t.Helper()
	ctx := t.Context()
	cleanup := seedArchiveParent(t, pool, document)
	defer cleanup()
	seedArchiveMember(t, pool, document)
	url := base + "/api/study/document/" + document + "/content?course_id=101"
	archiveContentProbe(t, url, 200)
	for _, mutation := range archiveContentMutations() {
		if _, err := pool.Native.ExecContext(ctx, mutation.Break); err != nil {
			t.Fatal(err)
		}
		archiveContentProbe(t, url, 404)
		if _, err := pool.Native.ExecContext(ctx, mutation.Repair); err != nil {
			t.Fatal(err)
		}
		archiveContentProbe(t, url, 200)
	}
	if _, err := pool.Native.ExecContext(
		ctx,
		`INSERT INTO archive_members(child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,depth,parent_source_hash,parent_source_fingerprint,member_hash,crc32,compressed_size,expanded_size,member_kind)
 SELECT 'fixture-archive-parent',id,'cycle.zip','cycle.zip','["cycle.zip"]',0,source_hash,'fixture',source_hash,0,1,1,'archive' FROM documents WHERE id=?`,
		document,
	); err != nil {
		t.Fatal(err)
	}
	archiveContentProbe(t, url, 404)
	if _, err := pool.Native.ExecContext(ctx, `DELETE FROM archive_members WHERE child_document_id='fixture-archive-parent'`); err != nil {
		t.Fatal(err)
	}
	archiveContentProbe(t, url, 200)
}

func seedArchiveParent(t *testing.T, pool *fixtureStore, document string) func() {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Native.ExecContext(
		ctx,
		`INSERT INTO documents(id,course_id,course_name,source_path,normalized_path,display_name,source_hash,document_kind,status,source_origin,content_hash_verified)
 SELECT 'fixture-archive-parent',course_id,course_name,'/fixture/parent.zip','/fixture/parent.zip','parent.zip',source_hash,'archive','ready',source_origin,1 FROM documents WHERE id=?`,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Native.ExecContext(ctx, `INSERT INTO document_revisions(id,document_id,course_id,logical_path,object_id)
 SELECT 'fixture-parent-revision','fixture-archive-parent',r.course_id,'/fixture/parent.zip',r.object_id FROM document_revisions r JOIN documents d ON d.id=r.document_id JOIN objects o ON o.id=r.object_id WHERE d.id=? AND o.sha256=d.source_hash AND r.deleted_at IS NULL LIMIT 1`, document)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := pool.Native.ExecContext(ctx, `DELETE FROM document_revisions WHERE id='fixture-parent-revision'`); err != nil {
			t.Error(err)
		}
		if _, err := pool.Native.ExecContext(ctx, `DELETE FROM documents WHERE id='fixture-archive-parent'`); err != nil {
			t.Error(err)
		}
	}
}

func seedArchiveMember(t *testing.T, pool *fixtureStore, document string) {
	t.Helper()
	_, err := pool.Native.ExecContext(
		t.Context(),
		`INSERT INTO archive_members(child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,depth,parent_source_hash,parent_source_fingerprint,member_hash,crc32,compressed_size,expanded_size,member_kind)
 SELECT id,'fixture-archive-parent','notes.txt','notes.txt','["notes.txt"]',0,source_hash,'fixture',source_hash,0,1,1,'text' FROM documents WHERE id=?`,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func archiveContentMutations() []struct{ Break, Repair string } {
	return []struct{ Break, Repair string }{
		{`UPDATE documents SET is_current=0 WHERE id='fixture-archive-parent'`, `UPDATE documents SET is_current=1 WHERE id='fixture-archive-parent'`},
		{`UPDATE archive_members SET member_hash='wrong' WHERE parent_document_id='fixture-archive-parent'`, `UPDATE archive_members SET member_hash=(SELECT d.source_hash FROM documents d WHERE d.id=archive_members.child_document_id) WHERE parent_document_id='fixture-archive-parent'`},
		{`UPDATE document_revisions SET deleted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id='fixture-parent-revision'`, `UPDATE document_revisions SET deleted_at=NULL WHERE id='fixture-parent-revision'`},
	}
}

func archiveContentProbe(t *testing.T, url string, expected int) {
	t.Helper()
	r := request(t, url, nil)
	r.Body.Close()
	if r.StatusCode != expected {
		t.Fatal("archive content admission", r.StatusCode, expected)
	}
}
