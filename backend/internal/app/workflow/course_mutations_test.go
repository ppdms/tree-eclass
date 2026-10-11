package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"tree-eclass/internal/domain/courses"
)

func destructiveChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := t.Context()
	seedCourseMutation(t, pool)
	mutationHTTP(t, base, "reset", "", "reset-1", 428)
	mutationHTTP(t, base, "reset", "reset:201", "", 400)
	// SQLite serializes writers implicitly, so no lock-conflict probe applies.
	mutationHTTP(t, base, "reset", "reset:201", "reset-1", 200)
	mutationHTTP(t, base, "reset", "reset:201", "reset-1", 409)
	for table, want := range map[string]int64{"nodes": 0, "announcements": 0, "study_annotations": 1, "courses": 1} {
		var count int64
		column := "course_id"
		if table == "courses" {
			column = "id"
		}
		if err := pool.Native.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+column+"=201").Scan(&count); err != nil ||
			count != want {
			t.Fatal("reset crossed learner boundary", table, count, err)
		}
	}
	var eclass, external int64
	if err := pool.Native.QueryRowContext(ctx, `SELECT max(CASE WHEN source_origin='eclass' THEN is_current END),max(CASE WHEN source_origin='external' THEN is_current END) FROM documents WHERE course_id=201`).Scan(&eclass, &external); err != nil ||
		eclass != 0 ||
		external != 1 {
		t.Fatal("reset lost user uploads or kept upstream current", err)
	}
	if _, err := pool.Native.ExecContext(ctx, `CREATE TRIGGER synthetic_delete_failure BEFORE DELETE ON courses BEGIN SELECT RAISE(ABORT,'synthetic rollback check'); END`); err != nil {
		t.Fatal(err)
	}
	service := courses.Service{Pool: pool}
	if err := service.Destructive(ctx, 201, "delete", "delete-1"); err == nil {
		t.Fatal("injected deletion failure did not abort")
	}
	var count int64
	if err := pool.Native.QueryRowContext(ctx, `SELECT count(*) FROM documents WHERE course_id=201`).Scan(&count); err != nil ||
		count != 2 {
		t.Fatal("failed deletion lost evidence", err)
	}
	if _, err := pool.Native.ExecContext(ctx, `DROP TRIGGER synthetic_delete_failure`); err != nil {
		t.Fatal(err)
	}
	mutationHTTP(t, base, "delete", "delete:201", "delete-1", 200)
	for _, table := range []string{"documents", "study_annotations", "document_revisions"} {
		if err := pool.Native.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE course_id=201").Scan(&count); err != nil ||
			count != 0 {
			t.Fatal("delete left scoped data", table, count, err)
		}
	}
	if err := pool.Native.QueryRowContext(ctx, `SELECT count(*) FROM document_revisions r JOIN objects o ON o.id=r.object_id WHERE r.course_id=202`).Scan(&count); err != nil ||
		count != 1 {
		t.Fatal("deletion damaged another course's shared object", err)
	}
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO courses(id,name,webdav_folder) VALUES(201,'Recreated','/Courses/201')`); err != nil {
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
	ctx := t.Context()
	hash := strings.Repeat("a", 64)
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO courses(id,name,webdav_folder) VALUES(201,'Reset target','/Courses/201'),(202,'Preserved','/Courses/202')`, nil},
		{`INSERT INTO nodes(course_id,name,url,local_path) VALUES(201,'root','url','/Courses/201/eclass')`, nil},
		{`INSERT INTO announcements(course_id,announcement_id,title,link) VALUES(201,'notice','Notice','url')`, nil},
		{`INSERT INTO study_annotations(course_id,document_id,page_number,body) VALUES(201,'eclass-reset-doc',1,'Keep learner note during reset')`, nil},
		{`INSERT INTO documents(id,course_id,course_name,source_path,normalized_path,display_name,source_hash,document_kind,status,source_origin) VALUES('eclass-reset-doc',201,'Reset target','/eclass','/eclass','eclass',?,'text','ready','eclass'),('external-delete-doc',201,'Reset target','/external','/external','external',?,'text','ready','external')`, []any{hash, hash}},
		{`INSERT INTO objects(id,bucket,key,version_id,sha256,bytes,media_type) VALUES('shared-delete-object','durable','shared','v1',?,9,'text/plain')`, []any{hash}},
		{`INSERT INTO document_revisions(id,document_id,course_id,logical_path,object_id) VALUES('delete-rev','external-delete-doc',201,'/external','shared-delete-object'),('keep-rev','unrelated-doc',202,'/external','shared-delete-object')`, nil},
	}
	for _, s := range statements {
		if _, err := pool.Native.ExecContext(ctx, s.query, s.args...); err != nil {
			t.Fatal(err)
		}
	}
}
