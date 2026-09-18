package library

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRegistrySchemas(t *testing.T) {
	r, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions()) != 14 {
		t.Fatal("lost a public tool")
	}
	for _, test := range []struct{ name, args string }{
		{"list_courses", "null"}, {"list_courses", `{"unknown":true}`},
		{"get_study_priorities", `{"limit":21}`}, {"list_materials", `{"course_id":0}`},
		{"read_material", `{"document_id":"a","locators":["page:1"]}`},
		{"search_materials", `{"query":"q","retrieval_mode":"invalid"}`},
	} {
		if _, err = r.Call(t.Context(), test.name, json.RawMessage(test.args)); !errors.Is(err, ErrArguments) {
			t.Fatal(test, err)
		}
	}
	tool := r.tools["get_course_study_blueprint"]

	tool.Handler = handlerFor(courseInput{}, func(_ context.Context, in courseInput) (any, error) { return in.ID, nil })
	r.tools["get_course_study_blueprint"] = tool
	value, err := r.Call(t.Context(), "get_course_study_blueprint", json.RawMessage(`{"course_id":9007199254740993}`))
	if err != nil || value != int64(9007199254740993) {
		t.Fatal("integer identity lost", value, err)
	}
}
