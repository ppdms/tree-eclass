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
		if _, err := pool.Native.Exec(ctx, mutation.Break); err != nil {
			t.Fatal(err)
		}
		archiveContentProbe(t, url, 404)
		if _, err := pool.Native.Exec(ctx, mutation.Repair); err != nil {
			t.Fatal(err)
		}
		archiveContentProbe(t, url, 200)
	}
	if _, err := pool.Native.Exec(
		ctx,
		`INSERT INTO knowledge.archive_members(child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,depth,parent_source_hash,parent_source_fingerprint,member_hash,crc32,compressed_size,expanded_size,member_kind)
 SELECT 'fixture-archive-parent',id,'cycle.zip','cycle.zip','["cycle.zip"]',0,source_hash,'fixture',source_hash,0,1,1,'archive' FROM knowledge.documents WHERE id=$1`,
		document,
	); err != nil {
		t.Fatal(err)
	}
	archiveContentProbe(t, url, 404)
	if _, err := pool.Native.Exec(ctx, `DELETE FROM knowledge.archive_members WHERE child_document_id='fixture-archive-parent'`); err != nil {
		t.Fatal(err)
	}
	archiveContentProbe(t, url, 200)
}

func seedArchiveParent(t *testing.T, pool *fixtureStore, document string) func() {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Native.Exec(
		ctx,
		`INSERT INTO knowledge.documents(id,course_id,course_name,source_path,normalized_path,display_name,source_hash,document_kind,status,source_origin,content_hash_verified)
 SELECT 'fixture-archive-parent',course_id,course_name,'/fixture/parent.zip','/fixture/parent.zip','parent.zip',source_hash,'archive','ready',source_origin,1 FROM knowledge.documents WHERE id=$1`,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Native.Exec(ctx, `INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id)
 SELECT 'fixture-parent-revision','fixture-archive-parent',r.course_id,'/fixture/parent.zip',r.object_id FROM app.document_revisions r JOIN knowledge.documents d ON d.id=r.document_id JOIN app.objects o ON o.id=r.object_id WHERE d.id=$1 AND o.sha256=d.source_hash AND r.deleted_at IS NULL LIMIT 1`, document)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		_, err := pool.Native.Exec(
			ctx,
			`DELETE FROM app.document_revisions WHERE id='fixture-parent-revision'; DELETE FROM knowledge.documents WHERE id='fixture-archive-parent'`,
		)
		if err != nil {
			t.Error(err)
		}
	}
}

func seedArchiveMember(t *testing.T, pool *fixtureStore, document string) {
	t.Helper()
	_, err := pool.Native.Exec(
		t.Context(),
		`INSERT INTO knowledge.archive_members(child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,depth,parent_source_hash,parent_source_fingerprint,member_hash,crc32,compressed_size,expanded_size,member_kind)
 SELECT id,'fixture-archive-parent','notes.txt','notes.txt','["notes.txt"]',0,source_hash,'fixture',source_hash,0,1,1,'text' FROM knowledge.documents WHERE id=$1`,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func archiveContentMutations() []struct{ Break, Repair string } {
	return []struct{ Break, Repair string }{
		{`UPDATE knowledge.documents SET is_current=0 WHERE id='fixture-archive-parent'`, `UPDATE knowledge.documents SET is_current=1 WHERE id='fixture-archive-parent'`},
		{`UPDATE knowledge.archive_members SET member_hash='wrong' WHERE parent_document_id='fixture-archive-parent'`, `UPDATE knowledge.archive_members m SET member_hash=d.source_hash FROM knowledge.documents d WHERE d.id=m.child_document_id AND m.parent_document_id='fixture-archive-parent'`},
		{`UPDATE app.document_revisions SET deleted_at=now() WHERE id='fixture-parent-revision'`, `UPDATE app.document_revisions SET deleted_at=NULL WHERE id='fixture-parent-revision'`},
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
