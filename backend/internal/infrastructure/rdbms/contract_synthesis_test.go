package rdbms_test

import (
	"context"
	"testing"
	"time"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

func TestSynthesisEvidenceCountsAndFreshRevisions(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 93, "contract"); err != nil {
				t.Fatal(err)
			}
			model := "contract-model"
			docVersion, pageVersion := settings.DocumentAnalysisVersion, settings.PageSynthesisVersion
			seedSynthesisEvidence(t, store, 93, model, docVersion, pageVersion)
			assertSynthesisEvidence(t, store, model, docVersion, pageVersion)
			courseHash := assertCourseRevision(t, store)
			assertPracticeRevision(t, store, courseHash)
		})
	}
}

func assertSynthesisEvidence(t *testing.T, store database.Store, model, docVersion, pageVersion string) {
	t.Helper()
	ctx := t.Context()
	params := database.SynthesisEvidenceParams{
		CourseID: 93, Model: model, DocVersion: docVersion, PageVersion: pageVersion, Limit: 10,
	}
	counts, err := store.Synthesis().EvidenceCounts(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 3 || counts.Ready != 2 {
		t.Fatalf("synthesis counts = %+v; want total 3 ready 2", counts)
	}
	docs, err := store.Synthesis().EvidenceDocuments(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 || docs[0].ID != "doc_synth_text" || docs[1].ID != "doc_synth_pdf" {
		t.Fatalf("synthesis evidence order = %+v", docs)
	}
	if docs[0].Version != docVersion || docs[1].Version != pageVersion {
		t.Fatalf("synthesis evidence versions = %q %q", docs[0].Version, docs[1].Version)
	}
	for _, doc := range docs {
		if doc.Requested != model || doc.Payload == "" {
			t.Fatalf("synthesis evidence row = %+v", doc)
		}
	}
}

func assertCourseRevision(t *testing.T, store database.Store) string {
	t.Helper()
	ctx := t.Context()
	past, courseHash := synthesisPast(), identity.TextHash("synthesis-course-revision")
	if err := store.Synthesis().QueueCourseRevision(ctx, database.QueueCourseParams{
		CourseID: 93, RevisionHash: courseHash, EvidenceHash: identity.TextHash("course-evidence"),
		PacketJSON: `{"course":93}`, Version: settings.CourseAnalysisVersion,
		Model: "contract-course", AvailableAt: past,
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.Synthesis().ClaimDueRow(ctx, database.SynthesisCourse,
		"contract-course", settings.CourseAnalysisVersion)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := store.Synthesis().LoadPacket(ctx, database.SynthesisCourse, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	if packet.Hash != courseHash || packet.PacketJSON != `{"course":93}` || packet.Attempts != 0 {
		t.Fatalf("course packet = %+v", packet)
	}
	claimedAt := synthesisPast()
	if err := store.Synthesis().MarkClaimed(ctx, database.SynthesisCourse, claim.ID, claimedAt); err != nil {
		t.Fatal(err)
	}
	active, err := store.Synthesis().ClaimActive(ctx, database.SynthesisCourse, claim.ID, claimedAt)
	if err != nil || !active {
		t.Fatalf("course claim active = %v, %v", active, err)
	}
	return courseHash
}

func assertPracticeRevision(t *testing.T, store database.Store, courseHash string) {
	t.Helper()
	ctx := t.Context()
	practiceHash := identity.TextHash("synthesis-practice-revision")
	if err := store.Synthesis().QueuePracticeRevision(ctx, database.QueuePracticeParams{
		CourseID: 93, UnitKey: "unit-a", Revision: courseHash, SetHash: practiceHash,
		EvidenceHash: identity.TextHash("practice-evidence"), PacketJSON: `{"unit":"a"}`,
		Version: settings.PracticeAnalysisVersion, Model: "contract-practice", AvailableAt: synthesisPast(),
	}); err != nil {
		t.Fatal(err)
	}
	practice, err := store.Synthesis().ClaimDueRow(ctx, database.SynthesisPractice,
		"contract-practice", settings.PracticeAnalysisVersion)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := store.Synthesis().LoadPacket(ctx, database.SynthesisPractice, practice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if packet.Hash != practiceHash || packet.UnitKey != "unit-a" ||
		packet.Blueprint != courseHash || packet.PacketJSON != `{"unit":"a"}` {
		t.Fatalf("practice packet = %+v", packet)
	}
}

func synthesisPast() string {
	return time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
}

func seedSynthesisEvidence(t *testing.T, store database.Store, course int64, model, docVersion, pageVersion string) {
	t.Helper()
	seedSynthesisDocument(t, store, course, "doc_synth_text", "notes.txt", "text", model, docVersion, "essential")
	seedSynthesisDocument(t, store, course, "doc_synth_pdf", "slides.pdf", "pdf", model, pageVersion, "useful")
	seedSynthesisDocument(t, store, course, "doc_synth_stale", "draft.txt", "text", model, "stale-version", "essential")
}

func seedSynthesisDocument(t *testing.T, store database.Store, course int64, id, name, kind, model, version string,
	weight string) {
	t.Helper()
	ctx := t.Context()
	path, hash := "/synth/"+name, identity.TextHash(id+name)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Indexing().ObserveDocument(ctx, database.ObserveDocumentParams{
		ID: id, CourseID: course, CourseName: "contract", SourcePath: path, NormalizedPath: path,
		DisplayName: identity.Encode(name), SourceHash: hash, DocumentKind: kind,
	}); err != nil {
		t.Fatal(err)
	}
	if err := seedSynthesisRevision(ctx, tx, "rev_"+id, id, course, path, hash); err != nil {
		t.Fatal(err)
	}
	if err := tx.Indexing().MarkIndexed(ctx, database.MarkIndexedParams{ID: id, WarningsJSON: "[]"}); err != nil {
		t.Fatal(err)
	}
	publishSynthesisInsight(t, ctx, tx, id, course, hash, kind, model, version, weight)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func publishSynthesisInsight(t *testing.T, ctx context.Context, tx database.Tx, id string, course int64, hash string,
	kind, model, version, weight string) {
	t.Helper()
	current, err := tx.Analysis().CurrentDocument(ctx, id, false)
	if err != nil {
		t.Fatal(err)
	}
	contextHash := database.AnalysisContextHash(id, course, hash, kind, current.Name,
		current.CourseName, current.Path, current.Origin)
	if err := tx.Analysis().QueueDocument(ctx, database.QueueDocumentParams{
		DocumentID: id, SourceHash: hash, ContextHash: contextHash, Version: version,
		Model: model, AvailableAt: synthesisPast(),
	}); err != nil {
		t.Fatal(err)
	}
	claimedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.Analysis().ClaimDocument(ctx, id, claimedAt); err != nil {
		t.Fatal(err)
	}
	payload := `{"summary":"ready ` + id + `","course_alignment":"aligned","importance":"` + weight + `"}`
	if err := tx.Analysis().PublishReady(ctx, database.AnalysisPublishParams{
		DocumentID: id, ClaimedAt: claimedAt, Model: model, Requested: model, Payload: payload,
		GeneratedAt: claimedAt, Hash: hash, ContextHash: contextHash, Version: version,
	}); err != nil {
		t.Fatal(err)
	}
}

func seedSynthesisRevision(ctx context.Context, tx database.Tx, rev, doc string, course int64, path, hash string,
) error {
	if err := tx.Objects().RegisterObject(ctx, database.ObjectReference{
		Bucket: "contract", Key: hash, VersionID: hash, SHA256: hash, Bytes: 8, MediaType: "text/plain",
	}); err != nil {
		return err
	}
	return tx.Objects().RegisterRevision(ctx, database.RegisterRevisionParams{
		ID: rev, DocumentID: doc, CourseID: course, LogicalPath: path, ObjectID: hash,
	})
}
