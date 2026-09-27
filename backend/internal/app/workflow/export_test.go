package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

type snapshotWriter struct {
	bytes.Buffer
	first func()
}

func (w *snapshotWriter) Write(p []byte) (int, error) {
	if w.first != nil {
		first := w.first
		w.first = nil
		first()
	}
	return w.Buffer.Write(p)
}

type failedExportWriter struct{}

func (failedExportWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestNativeLearnerExport(t *testing.T) {
	t.Parallel()
	c := nativeSharedController(t)
	conn, _ := startTestStorage(t, c)
	defer conn.Close(t.Context())
	pool, err := pgxpool.New(t.Context(), c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	seedLearnerExport(t, pool)
	service := settings.Service{Pool: pool}
	out := &snapshotWriter{first: func() {
		_, err := pool.Exec(
			t.Context(),
			`UPDATE app.courses SET name='later'; INSERT INTO app.study_sessions(course_id,note) VALUES(101,'later')`,
		)
		if err != nil {
			t.Fatal(err)
		}
	}}
	if err = service.Export(t.Context(), out); err != nil {
		t.Fatal(err)
	}
	verifyLearnerExport(t, out)
	if err = service.Export(t.Context(), failedExportWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("writer failure was lost", err)
	}
	exportHTTPCheck(t, c)
}

func verifyLearnerExport(t *testing.T, out *snapshotWriter) {
	t.Helper()
	var data struct {
		Courses []struct {
			Name string `json:"name"`
		} `json:"courses"`
		Study         map[string][]map[string]any `json:"study"`
		Conversations []struct {
			Messages []map[string]any `json:"messages"`
		} `json:"conversations"`
		Settings struct {
			Plans []struct {
				Enabled bool `json:"enabled"`
			} `json:"course_exam_plans"`
		} `json:"settings"`
	}
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		t.Fatal(err)
	}
	if len(data.Courses) != 1 || data.Courses[0].Name != "Κρυφό\x00\ue000" || len(data.Study["study_sessions"]) != 1 {
		t.Fatal("export combined different database snapshots")
	}
	if len(data.Study) != 11 {
		t.Fatal("learner table omitted")
	}
	for name, rows := range data.Study {
		if len(rows) == 0 {
			t.Fatalf("missing learner records: %s", name)
		}
	}
	if len(data.Settings.Plans) != 1 || !data.Settings.Plans[0].Enabled {
		t.Fatal("hidden exam commitment omitted")
	}
	annotations := data.Study["annotations"]
	if len(annotations) != 2 || annotations[0]["id"] != json.Number("9007199254740993") ||
		annotations[0]["body"] != "Σημείωση\x00\ue000" ||
		annotations[0]["status"] != "deleted" ||
		annotations[1]["status"] != "orphaned" {
		t.Fatal("annotation history, identity or text was lost")
	}
	if len(data.Conversations) != 1 || len(data.Conversations[0].Messages) != 2 ||
		data.Conversations[0].Messages[1]["content"] != "Απάντηση\x00" {
		t.Fatal("conversation turn was not exported intact")
	}
	for _, secret := range []string{"private-eclass", "private-discord", "private-cookie", "private-hook", "private-provider"} {
		if bytes.Contains(out.Bytes(), []byte(secret)) {
			t.Fatal("export included a secret")
		}
	}
}

func exportHTTPCheck(t *testing.T, c *Controller) {
	t.Helper()
	temp := t.TempDir()
	api, err := server.New(
		t.Context(),
		server.Config{
			DatabaseURL: c.databaseURL(),
			ObjectsRoot: c.testObjectsRoot(),
			Temp:        temp,
			Mode:        "test",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()
	response := request(t, httpServer.URL+"/api/settings/export", nil)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || !json.Valid(body) ||
		!strings.Contains(response.Header.Get("Content-Disposition"), "attachment") {
		t.Fatal("export HTTP delivery", response.StatusCode, err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatal("export spool was retained", err)
	}
}

func seedLearnerExport(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
INSERT INTO app.courses(id,name,webdav_folder,hidden) VALUES(101,'hidden','/Courses/101',1);
INSERT INTO app.course_exam_plans(course_id,enabled,exam_at) VALUES(101,1,'2026-09-20T10:00');
INSERT INTO app.study_sessions(course_id,note) VALUES(101,'original');
INSERT INTO app.study_unit_events(course_id,plan_revision,action_id,unit_key,event_type) VALUES(101,'rev','action','unit','completed');
INSERT INTO app.study_plan_items(course_id,scheduled_date,kind) VALUES(101,'2026-09-20','study');
INSERT INTO app.study_review_overrides(course_id,review_offset,scheduled_date) VALUES(101,1,'2026-09-20');
INSERT INTO app.file_study(course_id,file_path,level) VALUES(101,'file',3);
INSERT INTO app.collapsed_course_folders(course_id,folder_key,collapsed) VALUES(101,'root',1);
INSERT INTO app.practice_attempts(course_id,unit_key,question_id,outcome) VALUES(101,'unit','question','correct');
INSERT INTO app.study_workspace_sessions(id,course_id,client_session_key) VALUES(1,101,'synthetic');
INSERT INTO app.study_reading_spans(session_id,course_id,document_id,page_number) VALUES(1,101,'doc',1);
INSERT INTO app.study_reading_beats(session_id,sequence) VALUES(1,0);
INSERT INTO app.study_annotations(id,course_id,document_id,page_number,status,tags_json) VALUES(9007199254740993,101,'doc',1,'deleted','["Επανάληψη"]'),(9007199254740994,101,'doc',2,'orphaned','[]');
INSERT INTO app.chat_conversations(id,title) VALUES(1,'Συζήτηση');
INSERT INTO app.chat_messages(conversation_id,role,content,consulted_json) VALUES(1,'user','Ερώτηση','[]'),(1,'assistant','answer','[{"tool":"read_material"}]');
INSERT INTO app.credentials(id,username,password) VALUES(1,'synthetic','private-eclass');
INSERT INTO app.app_data(key,value) VALUES('cookie',convert_to('private-cookie','UTF8'));
INSERT INTO app.discord_export_settings(id,token) VALUES(1,'private-discord');
`)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct{ Query, Text string }{
		{`UPDATE app.courses SET name=$1`, "Κρυφό\x00\ue000"},
		{`UPDATE app.study_annotations SET body=$1`, "Σημείωση\x00\ue000"},
		{`UPDATE app.chat_messages SET content=$1 WHERE role='assistant'`, "Απάντηση\x00"},
	} {
		if _, err = pool.Exec(t.Context(), change.Query, identity.Encode(change.Text)); err != nil {
			t.Fatal(err)
		}
	}
}
