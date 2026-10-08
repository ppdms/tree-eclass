package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"tree-eclass/internal/domain/courses"
)

func destructiveChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := t.Context()
	seedCourseMutation(t, pool)
	mutationHTTP(t, base, "reset", "", "reset-1", 428)
	mutationHTTP(t, base, "reset", "reset:201", "", 400)
	lock, err := pool.Native.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lock.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('eclass-sync:201',0))`); err != nil {
		lock.Release()
		t.Fatal(err)
	}
	mutationHTTP(t, base, "reset", "reset:201", "reset-1", 409)
	if _, err = lock.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended('eclass-sync:201',0))`); err != nil {
		lock.Release()
		t.Fatal(err)
	}
	lock.Release()
	mutationHTTP(t, base, "reset", "reset:201", "reset-1", 200)
	mutationHTTP(t, base, "reset", "reset:201", "reset-1", 409)
	for table, want := range map[string]int64{"app.nodes": 0, "app.announcements": 0, "app.study_annotations": 1, "app.courses": 1} {
		var count int64
		column := "course_id"
		if table == "app.courses" {
			column = "id"
		}
		if err = pool.Native.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE "+column+"=201").Scan(&count); err != nil ||
			count != want {
			t.Fatal("reset crossed learner boundary", table, count, err)
		}
	}
	var eclass, external int64
	if err = pool.Native.QueryRow(ctx, `SELECT max(is_current) FILTER(WHERE source_origin='eclass'),max(is_current) FILTER(WHERE source_origin='external') FROM knowledge.documents WHERE course_id=201`).Scan(&eclass, &external); err != nil ||
		eclass != 0 ||
		external != 1 {
		t.Fatal("reset lost user uploads or kept upstream current", err)
	}
	if _, err = pool.Native.Exec(ctx, `CREATE FUNCTION app.synthetic_delete_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback check'; END $$;
CREATE TRIGGER synthetic_delete_failure BEFORE DELETE ON app.courses FOR EACH ROW EXECUTE FUNCTION app.synthetic_delete_failure()`); err != nil {
		t.Fatal(err)
	}
	service := courses.Service{Pool: pool}
	if err = service.Destructive(ctx, 201, "delete", "delete-1"); err == nil {
		t.Fatal("injected deletion failure did not abort")
	}
	var count int64
	if err = pool.Native.QueryRow(ctx, `SELECT count(*) FROM knowledge.documents WHERE course_id=201`).Scan(&count); err != nil ||
		count != 2 {
		t.Fatal("failed deletion lost evidence", err)
	}
	if _, err = pool.Native.Exec(ctx, `DROP TRIGGER synthetic_delete_failure ON app.courses; DROP FUNCTION app.synthetic_delete_failure()`); err != nil {
		t.Fatal(err)
	}
	mutationHTTP(t, base, "delete", "delete:201", "delete-1", 200)
	for _, table := range []string{"knowledge.documents", "app.study_annotations", "app.document_revisions"} {
		if err = pool.Native.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE course_id=201").Scan(&count); err != nil ||
			count != 0 {
			t.Fatal("delete left scoped data", table, count, err)
		}
	}
	if err = pool.Native.QueryRow(ctx, `SELECT count(*) FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id WHERE r.course_id=202`).Scan(&count); err != nil ||
		count != 1 {
		t.Fatal("deletion damaged another course's shared object", err)
	}
	if _, err = pool.Native.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(201,'Recreated','/Courses/201')`); err != nil {
		t.Fatal(err)
	}
	mutationHTTP(t, base, "delete", "delete:201", "delete-1", 409)
}

func mutationHTTP(t *testing.T, base, action, confirmation, key string, status int) {
	t.Helper()
	encoded, _ := json.Marshal(map[string]any{})
	r, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/courses/201/%s", base, action), bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Tree-Eclass-Confirmation", confirmation)
	r.Header.Set("X-Idempotency-Key", key)
	client := http.Client{}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("%s: %d %s", action, response.StatusCode, body)
	}
}

func seedCourseMutation(t *testing.T, pool *fixtureStore) {
	t.Helper()
	_, err := pool.Native.Exec(
		t.Context(),
		`INSERT INTO app.courses(id,name,webdav_folder) VALUES(201,'Reset target','/Courses/201'),(202,'Preserved','/Courses/202');
INSERT INTO app.nodes(course_id,name,url,local_path) VALUES(201,'root','url','/Courses/201/eclass');
INSERT INTO app.announcements(course_id,announcement_id,title,link) VALUES(201,'notice','Notice','url');
INSERT INTO app.study_annotations(course_id,document_id,page_number,body) VALUES(201,'eclass-reset-doc',1,'Keep learner note during reset');
INSERT INTO knowledge.documents(id,course_id,course_name,source_path,normalized_path,display_name,source_hash,document_kind,status,source_origin)
VALUES('eclass-reset-doc',201,'Reset target','/eclass','/eclass','eclass',repeat('a',64),'text','ready','eclass'),('external-delete-doc',201,'Reset target','/external','/external','external',repeat('a',64),'text','ready','external');
INSERT INTO app.objects(id,bucket,key,version_id,sha256,bytes,media_type) VALUES('shared-delete-object','durable','shared','v1',repeat('a',64),9,'text/plain');
INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id) VALUES('delete-rev','external-delete-doc',201,'/external','shared-delete-object'),('keep-rev','unrelated-doc',202,'/external','shared-delete-object');`,
	)
	if err != nil {
		t.Fatal(err)
	}
}
