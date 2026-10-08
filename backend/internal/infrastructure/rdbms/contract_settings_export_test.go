package rdbms_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/settings"
	chatservice "tree-eclass/internal/services/chat"
)

func TestSettingsExportExcludesSecretsAndStreamsChats(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			seedExportCourses(t, store)
			seedExportStored2(t, backend)
			seedExportSecrets(t, store)
			seedExportConversation(t, store)
			assertExportExcludesSecrets(t, store)
			assertMaterialModelNormalization(t, store)
		})
	}
}

func seedExportCourses(t *testing.T, store database.Store) {
	t.Helper()
	service := courses.Service{Pool: store}
	if err := service.Add(t.Context(), 101, "Κρυφό\x00\ue000", "Σ\x00"); err != nil {
		t.Fatal(err)
	}
	if err := service.Add(t.Context(), 102, "plain course"); err != nil {
		t.Fatal(err)
	}
}

func seedExportStored2(t *testing.T, backend contractBackend) {
	t.Helper()
	ctx := t.Context()
	if backend.name == "sqlite" {
		db, err := sql.Open("sqlite", backend.cfg.SQLitePath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		query := `INSERT INTO course_exam_plans(course_id,enabled,exam_at) VALUES(101,2,'2026-09-20T10:00')`
		if _, err = db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
		return
	}
	conn, err := pgx.Connect(ctx, backend.cfg.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	query := `INSERT INTO app.course_exam_plans(course_id,enabled,exam_at) VALUES(101,2,'2026-09-20T10:00')`
	if _, err = conn.Exec(ctx, query); err != nil {
		t.Fatal(err)
	}
}

func seedExportSecrets(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	service := settings.Service{Pool: store}
	if err := service.SaveCredentials(ctx, "export-user", "private-eclass", false); err != nil {
		t.Fatal(err)
	}
	if err := service.SaveWebhook(ctx, "https://example.invalid/private-hook", false); err != nil {
		t.Fatal(err)
	}
}

func seedExportConversation(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	service := chatservice.Store{Pool: store}
	saved, err := service.SaveTurn(ctx, chatservice.Turn{
		Question: "Ερώτηση\x00", Answer: "Απάντηση\x00",
		Model: "export-model", Consulted: []chatservice.Consultation{{Tool: "read_material"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SaveTurn(ctx, chatservice.Turn{
		ConversationID: &saved.ID, Question: "followup", Answer: "done",
		Model: "export-model", Consulted: []chatservice.Consultation{},
	})
	if err != nil || second.ID != saved.ID {
		t.Fatal("second turn did not reuse conversation", second, err)
	}
}

type exportCheck struct {
	Courses []struct {
		Name      string  `json:"name"`
		ShortName *string `json:"short_name"`
	} `json:"courses"`
	Settings struct {
		Plans []struct {
			CourseID int64   `json:"course_id"`
			Enabled  bool    `json:"enabled"`
			ExamAt   *string `json:"exam_at"`
		} `json:"course_exam_plans"`
	} `json:"settings"`
	Conversations []struct {
		ID       int64 `json:"id"`
		Messages []struct {
			ID        json.Number `json:"id"`
			Role      string      `json:"role"`
			Content   string      `json:"content"`
			Model     *string     `json:"model"`
			Consulted []struct {
				Tool string `json:"tool"`
			} `json:"consulted"`
		} `json:"messages"`
	} `json:"conversations"`
}

func exportBytes(t *testing.T, store database.Store) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := (settings.Service{Pool: store}).Export(t.Context(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func assertExportExcludesSecrets(t *testing.T, store database.Store) {
	t.Helper()
	raw := exportBytes(t, store)
	for _, secret := range []string{"private-eclass", "private-hook", "export-user"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("export included secret %q", secret)
		}
	}
	if strings.Contains(string(raw), "native_settings") {
		t.Fatal("export included native settings")
	}
	var data exportCheck
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		t.Fatal(err)
	}
	assertExportCourses(t, data)
	assertExportPlans(t, data)
	assertExportMessages(t, data)
}

func assertExportCourses(t *testing.T, data exportCheck) {
	t.Helper()
	if len(data.Courses) != 2 || data.Courses[0].Name != "Κρυφό\x00\ue000" {
		t.Fatalf("export course order/content = %+v", data.Courses)
	}
	if data.Courses[0].ShortName == nil || *data.Courses[0].ShortName != "Σ\x00" {
		t.Fatalf("export short name null/content = %+v", data.Courses[0].ShortName)
	}
}

func assertExportPlans(t *testing.T, data exportCheck) {
	t.Helper()
	if len(data.Settings.Plans) != 2 || data.Settings.Plans[0].CourseID != 101 {
		t.Fatalf("export exam plans = %+v", data.Settings.Plans)
	}
	if data.Settings.Plans[0].Enabled {
		t.Fatal("stored enabled=2 exported true; want false via coalesce =1")
	}
	if data.Settings.Plans[0].ExamAt == nil || *data.Settings.Plans[0].ExamAt != "2026-09-20T10:00" {
		t.Fatalf("export exam_at = %+v", data.Settings.Plans[0].ExamAt)
	}
}

func assertExportMessages(t *testing.T, data exportCheck) {
	t.Helper()
	if len(data.Conversations) != 1 || len(data.Conversations[0].Messages) != 4 {
		t.Fatalf("export conversations = %+v", data.Conversations)
	}
	messages := data.Conversations[0].Messages
	if messages[0].Content != "Ερώτηση\x00" || messages[0].Model != nil || len(messages[0].Consulted) != 0 {
		t.Fatalf("export user message = %+v", messages[0])
	}
	if messages[1].Content != "Απάντηση\x00" || messages[1].Model == nil ||
		*messages[1].Model != "export-model" || len(messages[1].Consulted) != 1 ||
		messages[1].Consulted[0].Tool != "read_material" {
		t.Fatalf("export assistant message = %+v", messages[1])
	}
	previous := ""
	for _, message := range messages {
		current := message.ID.String()
		if previous != "" && current <= previous {
			t.Fatalf("export message order = %v", messages)
		}
		previous = current
	}
}

func assertMaterialModelNormalization(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	service := settings.Service{Pool: store}
	config := settings.DefaultAI()
	config.Model = "  contract-model  "
	if err := service.SaveAI(ctx, config); err != nil {
		t.Fatal(err)
	}
	read, err := settings.ReadAI(ctx, store)
	if err != nil || read.Model != "contract-model" {
		t.Fatalf("domain AI normalization = %+v, %v", read, err)
	}
	doc := seedExportDocument(t, store)
	publishExportGuide(t, store, doc)
	items, err := (materials.Service{Pool: store}).List(ctx, 101, doc, false)
	if err != nil || len(items) != 1 || items[0].DocumentID != doc {
		t.Fatalf("materials list = %+v, %v", items, err)
	}
	if items[0].Type != "past_paper" || items[0].Classification != "ai" {
		t.Fatalf("normalized model did not select live AI hint: %+v", items[0])
	}
	invalid := settings.DefaultAI()
	invalid.Model = "has space"
	if err = service.SaveAI(ctx, invalid); err == nil {
		t.Fatal("invalid model accepted")
	}
}

func seedExportDocument(t *testing.T, store database.Store) string {
	t.Helper()
	ctx := t.Context()
	path := "/external/contract-model.txt"
	doc := identity.Document(101, path)
	hash := identity.TextHash("contract-model-bytes")
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encoded := identity.Encode(path)
	if err = tx.Indexing().ObserveDocument(ctx, database.ObserveDocumentParams{
		ID: doc, CourseID: 101, CourseName: identity.Encode("plain course"), SourcePath: encoded,
		NormalizedPath: encoded, DisplayName: identity.Encode("contract model"),
		SourceHash: hash, DocumentKind: "text",
	}); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Objects().RegisterObject(ctx, database.ObjectReference{
		Bucket: "materials", Key: hash, VersionID: hash, SHA256: hash, Bytes: 8,
	}); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Objects().RegisterRevision(ctx, database.RegisterRevisionParams{
		ID: "revision_contract_model", DocumentID: doc, CourseID: 101, LogicalPath: path, ObjectID: hash,
	}); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Indexing().MarkIndexed(ctx, database.MarkIndexedParams{ID: doc, WarningsJSON: "[]"}); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return doc
}

func publishExportGuide(t *testing.T, store database.Store, doc string) {
	t.Helper()
	tx, err := store.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	current, err := tx.Analysis().CurrentDocument(t.Context(), doc, false)
	if err != nil {
		t.Fatal(err)
	}
	contextHash := database.AnalysisContextHash(current.ID, current.CourseID, current.SourceHash, current.Kind,
		current.Name, current.CourseName, current.Path, current.Origin)
	claim := "2020-01-01T00:00:00.123456789Z"
	if err = tx.Analysis().QueueDocument(t.Context(), database.QueueDocumentParams{
		DocumentID: current.ID, SourceHash: current.SourceHash, ContextHash: contextHash,
		Version: settings.DocumentAnalysisVersion, Model: "contract-model", AvailableAt: claim,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Analysis().ClaimDocument(t.Context(), current.ID, claim); err != nil {
		t.Fatal(err)
	}
	if err = tx.Analysis().PublishReady(t.Context(), database.AnalysisPublishParams{
		DocumentID: current.ID, ClaimedAt: claim, Model: "served-fallback", Requested: "contract-model",
		Payload: `{"material_type":"past_exam"}`, GeneratedAt: claim, Hash: current.SourceHash,
		ContextHash: contextHash, Version: settings.DocumentAnalysisVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}
