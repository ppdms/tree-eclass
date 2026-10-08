package workflow

import (
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/settings"
)

func materialPresentationChecks(t *testing.T, pool rdbms.Pool, base, document string) {
	t.Helper()
	ctx := t.Context()
	var list struct {
		Materials []materials.Material `json:"materials"`
	}
	url := base + "/api/v1/courses/101/materials"
	apiJSON(t, "GET", url, nil, 200, &list)
	if len(list.Materials) != 1 || list.Materials[0].DocumentID != document ||
		list.Materials[0].Type != "student_notes" ||
		list.Materials[0].Classification != "manual" ||
		list.Materials[0].DownloadURL == nil ||
		list.Materials[0].Renderable {
		t.Fatal("external reader contract", list.Materials)
	}
	response := request(t, base+*list.Materials[0].DownloadURL, nil)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("presented download did not resolve")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO knowledge.document_enrichments(document_id,source_hash,analysis_version,status,model,payload_json,available_at)
SELECT id,source_hash,'6','ready',$2,'{"material_type":"past_exam"}','now' FROM knowledge.documents WHERE id=$1`, document, settings.DefaultAI().Model); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &list)
	if list.Materials[0].Type != "student_notes" {
		t.Fatal("AI overrode manual classification")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM app.external_material_metadata WHERE course_id=101`); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &list)
	if list.Materials[0].Type != "past_paper" || list.Materials[0].Classification != "ai" {
		t.Fatal("current AI classification not used")
	}
	if _, err := pool.Exec(ctx, `UPDATE knowledge.document_enrichments SET source_hash='stale' WHERE document_id=$1`, document); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &list)
	if list.Materials[0].Classification == "ai" {
		t.Fatal("stale source classification used")
	}
	if _, err := pool.Exec(ctx, `UPDATE knowledge.document_enrichments SET source_hash=(SELECT source_hash FROM knowledge.documents WHERE id=$1),payload_json='invalid JSON' WHERE document_id=$1`, document); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &list)
	if list.Materials[0].Classification == "ai" {
		t.Fatal("invalid classification used")
	}
	var updated struct {
		Material materials.Material `json:"material"`
	}
	apiJSON(t, "PATCH", url+"/"+document, map[string]string{"material_type": "study_guide"}, 200, &updated)
	if updated.Material.DocumentID != document || updated.Material.Classification != "manual" ||
		updated.Material.Type != "study_guide" ||
		updated.Material.DownloadURL == nil {
		t.Fatal("type edit returned incomplete material")
	}
	apiJSON(t, "PATCH", url+"/"+document, map[string]string{"material_type": "invalid"}, 422, nil)
	if _, err := pool.Exec(ctx, `UPDATE app.courses SET hidden=1 WHERE id=101`); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 404, nil)
	apiJSON(t, "PATCH", url+"/"+document, map[string]string{"material_type": "textbook"}, 404, nil)
	if _, err := pool.Exec(ctx, `UPDATE app.courses SET hidden=0 WHERE id=101; DELETE FROM knowledge.document_enrichments`); err != nil {
		t.Fatal(err)
	}
}
