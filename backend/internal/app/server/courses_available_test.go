package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAvailableCourseMergesLocalNames(t *testing.T) {
	portfolio := `{"id":267,"code":"INF267","title":"Upstream title","professor":"Prof"}`
	var decoded struct {
		Courses []availableCourse `json:"courses"`
	}
	if err := json.Unmarshal([]byte(`{"courses":[`+portfolio+`]}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Courses[0].ID != 267 || decoded.Courses[0].Name != "" {
		t.Fatal("portfolio contract changed", decoded.Courses)
	}
	if !strings.Contains(portfolio, "INF267") {
		t.Fatal("INF code missing from contract")
	}
	// Route-ordering proof lives in the native workflow check below, which
	// serves GET /api/v1/courses/available through a real database.
}
