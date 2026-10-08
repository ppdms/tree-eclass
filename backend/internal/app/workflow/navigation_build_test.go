package workflow

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func navigationBuildChecks(t *testing.T, pool rdbms.Pool, base, document string) {
	t.Helper()
	ctx := t.Context()
	a := settings.DefaultAI()
	payload, packet := navigationBuildPacket(t, pool, document, a)
	communityHash := blueprintCommunityFixture(t, pool)
	defer func() {
		if _, err := pool.Exec(ctx, `DELETE FROM messages.archive_sources WHERE path='blueprint-fixture'; DELETE FROM app.discord_course_channels WHERE root_channel_id='100001'`); err != nil {
			t.Error(err)
		}
	}()
	packet["source_snapshot"].(map[string]any)["conversations"] = []any{
		map[string]any{"conversation_id": "two", "content_hash": communityHash},
	}
	packetRaw, _ := json.Marshal(packet)
	_, err := pool.Exec(
		ctx,
		`INSERT INTO knowledge.course_blueprints(course_id,revision,revision_hash,evidence_hash,evidence_packet_json,analysis_version,status,requested_model,model,payload_json,available_at,created_at)
 VALUES(101,1,'build-r1','evidence-1',$1,$2,'ready',$3,'fallback-course-model',$4,'now','now')`,
		string(packetRaw),
		settings.CourseAnalysisVersion,
		a.CourseModel,
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	service := navigation.Service{Pool: pool}
	refresh := func() {
		t.Helper()
		for range 100 {
			changed, err := service.Refresh(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !changed {
				return
			}
		}
		t.Fatal("navigation generation debt did not settle")
	}
	refresh()
	navigationBuildReaderChecks(t, pool, base, document, refresh)
}

func navigationBuildPacket(t *testing.T, pool rdbms.Pool, document string, a settings.AI) (string, map[string]any) {
	t.Helper()
	ctx := t.Context()
	raw, err := os.ReadFile("../../domain/blueprints/testdata/validation.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture []struct {
		Expected json.RawMessage
		Packet   map[string]any
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	packetRaw, _ := json.Marshal(fixture[0].Packet)
	var packet map[string]any
	if err = json.Unmarshal([]byte(strings.ReplaceAll(string(packetRaw), "document:one", "document:"+document)), &packet); err != nil {
		t.Fatal(err)
	}
	var hash, insight string
	if err = pool.QueryRow(ctx, `SELECT d.source_hash,e.payload_json FROM knowledge.documents d JOIN knowledge.document_enrichments e ON e.document_id=d.id WHERE d.id=$1`, document).Scan(&hash, &insight); err != nil {
		t.Fatal(err)
	}
	payloadHash, err := blueprints.PayloadHash([]byte(insight))
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.ReplaceAll(string(fixture[0].Expected), "document:one", "document:"+document)
	packet["source_snapshot"] = map[string]any{
		"documents": []any{
			map[string]any{
				"document_id":                 document,
				"source_hash":                 hash,
				"enrichment_source_hash":      hash,
				"enrichment_analysis_version": settings.DocumentAnalysisVersion,
				"enrichment_model":            a.Model,
				"enrichment_payload_hash":     payloadHash,
			},
		},
	}
	return payload, packet
}

func navigationBuildReaderChecks(t *testing.T, pool rdbms.Pool, base, document string, refresh func()) {
	t.Helper()
	ctx := t.Context()
	var err error
	var view navigation.View
	apiJSON(t, "GET", base+"/api/v1/courses/101/overview", nil, 200, &view)
	if view.Blueprint["usable"] != true || view.Blueprint["revision_id"] != "build-r1" {
		t.Fatal("validated native blueprint unavailable", view)
	}
	var detail map[string]any
	apiJSON(t, "GET", base+"/api/v1/courses/101", nil, 200, &detail)
	if detail["course_blueprint"].(map[string]any)["revision_id"] != "build-r1" || detail["timeline"] == nil ||
		detail["study_distribution"] == nil {
		t.Fatal("legacy course detail contract lost", detail)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/999999", nil, 404, nil)
	var unit map[string]any
	apiJSON(t, "GET", base+"/api/v1/courses/101/roadmap/units/unit_one?revision=build-r1", nil, 200, &unit)
	actions := unit["unit"].(map[string]any)["actions"].([]any)
	if len(actions) != 1 || actions[0].(map[string]any)["title"] != "Διάβασε <x> & y" {
		t.Fatal("decorated action", actions)
	}
	studyEventChecks(t, pool, base, actions[0].(map[string]any)["action_id"].(string))
	studyProjectionChecks(t, pool, base, actions[0].(map[string]any)["action_id"].(string))
	practiceChecks(t, pool, base, document, refresh)
	workspaceContextChecks(t, pool, base, document, actions[0].(map[string]any)["action_id"].(string))
	apiJSON(t, "GET", base+"/api/v1/courses/101/roadmap/units/unit_one?revision=obsolete", nil, 409, nil)
	apiJSON(t, "GET", base+"/api/v1/courses/101/roadmap/units/missing?revision=build-r1", nil, 404, nil)
	var section map[string]any
	apiJSON(
		t,
		"GET",
		base+"/api/v1/courses/101/roadmap/sections/strategy-evidence?revision=build-r1",
		nil,
		200,
		&section,
	)
	links := section["roadmap"].(map[string]any)["exam_strategy"].(map[string]any)["evidence_links"].([]any)
	if len(links) != 1 || links[0].(map[string]any)["document_id"] != document {
		t.Fatal("exact evidence resolution", links)
	}
	navigationSourceMutationChecks(t, pool, base, document, refresh)
	if _, err = pool.Exec(ctx, `DELETE FROM knowledge.course_blueprints WHERE course_id=101;
 DELETE FROM read_model.navigation WHERE course_id=101; DELETE FROM read_model.roadmap_actions WHERE course_id=101`); err != nil {
		t.Fatal(err)
	}
}

func refreshNavigation(t *testing.T, pool rdbms.Pool) {
	t.Helper()
	for range 100 {
		changed, err := (navigation.Service{Pool: pool}).Refresh(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			return
		}
	}
	t.Fatal("navigation did not settle")
}

func navigationSourceMutationChecks(t *testing.T, pool rdbms.Pool, base, document string, refresh func()) {
	t.Helper()
	ctx := t.Context()
	name := "Δένδρα\x00\ue0000"
	if _, err := pool.Exec(ctx, `UPDATE app.courses SET name=$1 WHERE id=101`, identity.Encode(name)); err != nil {
		t.Fatal(err)
	}
	refresh()
	var view navigation.View
	apiJSON(t, "GET", base+"/api/v1/courses/101/roadmap", nil, 200, &view)
	if view.Course.Name != name || view.Blueprint["course_name"] != name {
		t.Fatal("navigation text codec lost identity", view)
	}
	if _, err := pool.Exec(ctx, `UPDATE knowledge.document_enrichments SET payload_json='{"summary":"Changed insight"}' WHERE document_id=$1`, document); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/101/overview", nil, 200, &view)
	if view.Blueprint["usable"] != false {
		t.Fatal("stale generation visible before refresh")
	}
	refresh()
	apiJSON(t, "GET", base+"/api/v1/courses/101/overview", nil, 200, &view)
	if view.Blueprint["usable"] != false || view.Blueprint["validation_reason"] != "cached_evidence_insight_stale" {
		t.Fatal("changed source insight accepted", view)
	}
	apiJSON(t, "GET", base+"/api/v1/courses/101/roadmap/units/unit_one?revision=build-r1", nil, 409, nil)
	if _, err := pool.Exec(ctx, `UPDATE app.courses SET name='Συνθετικό μάθημα' WHERE id=101`); err != nil {
		t.Fatal(err)
	}
}
